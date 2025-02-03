#include "azure_filesystem.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include <azure/storage/common/storage_exception.hpp>
#include <thread>

namespace duckdb {

static constexpr uint64_t DEFAULT_MAX_UPLOAD_THREADS = 50;

AzureContextState::AzureContextState(const AzureReadOptions &read_options)
    : read_options(read_options), is_valid(true) {
}

bool AzureContextState::IsValid() const {
	return is_valid;
}

void AzureContextState::QueryEnd() {
	is_valid = false;
}

AzureFileHandle::AzureFileHandle(AzureStorageFileSystem &fs, string path, FileOpenFlags flags,
                                 const AzureReadOptions &read_options)
// TODO: change back for v1.2.0
//  : FileHandle(fs, std::move(path)), flags(flags),
    : FileHandle(fs, std::move(path)), flags(flags),
      // File info
      length(0), last_modified(0),
      // Read info
      buffer_available(0), buffer_idx(0), file_offset(0), buffer_start(0), buffer_end(0),

      // Write info
      uploads_in_progress(0), parts_uploaded(0), upload_finalized(false),
      uploader_has_error(false), upload_exception(nullptr),

      // Options
      read_options(read_options) {
	if (!flags.RequireParallelAccess() && !flags.DirectIO()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[read_options.buffer_size]);
	}
}

bool AzureFileHandle::PostConstruct() {
	return static_cast<AzureStorageFileSystem &>(file_system).LoadFileInfo(*this);
}

bool AzureStorageFileSystem::LoadFileInfo(AzureFileHandle &handle) {
	if (handle.flags.OpenForReading()) {
		try {
			LoadRemoteFileInfo(handle);
		} catch (const Azure::Storage::StorageException &e) {
			auto status_code = int(e.StatusCode);
			if (status_code == 404 && handle.flags.ReturnNullIfNotExists()) {
				return false;
			}
			throw IOException(
			    "AzureBlobStorageFileSystem open file '%s' failed with code'%s', Reason Phrase: '%s', Message: '%s'",
			    handle.path, e.ErrorCode, e.ReasonPhrase, e.Message);
		} catch (const std::exception &e) {
			throw IOException(
			    "AzureBlobStorageFileSystem could not open file: '%s', unknown error occurred, this could mean "
			    "the credentials used were wrong. Original error message: '%s' ",
			    handle.path, e.what());
		}
	}
	return true;
}

AzureFileHandle::~AzureFileHandle() {
	if (Exception::UncaughtException()) {
		// We are in an exception, don't do anything
		return;
	}

	try {
		// Try to finalize the file
		Close();
	} catch (...) { // NOLINT
	}
}

unique_ptr<FileHandle> AzureStorageFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	auto handle = CreateHandle(path, flags, opener);
	return std::move(handle);
}

int64_t AzureStorageFileSystem::GetFileSize(FileHandle &handle) {
	auto &afh = handle.Cast<AzureFileHandle>();
	return afh.length;
}

time_t AzureStorageFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &afh = handle.Cast<AzureFileHandle>();
	return afh.last_modified;
}

void AzureStorageFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = handle.Cast<AzureFileHandle>();
	sfh.file_offset = location;
}

// TODO: this code is identical to HTTPFS, look into unifying it
void AzureStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<AzureFileHandle>();

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags.DirectIO() || hfh.flags.RequireParallelAccess()) {
		if (to_read == 0) {
			return;
		}
		ReadRange(hfh, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.read_options.buffer_size, hfh.length - hfh.file_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				ReadRange(hfh, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				ReadRange(hfh, hfh.file_offset, (char *)hfh.read_buffer.get(), new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t AzureStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = handle.Cast<AzureFileHandle>();
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

shared_ptr<AzureContextState> AzureStorageFileSystem::GetOrCreateStorageContext(optional_ptr<FileOpener> opener,
                                                                                const string &path,
                                                                                const AzureParsedUrl &parsed_url) {
	Value value;
	bool azure_context_caching = true;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_context_caching", value)) {
		azure_context_caching = value.GetValue<bool>();
	}
	auto client_context = FileOpener::TryGetClientContext(opener);

	shared_ptr<AzureContextState> result;
	if (azure_context_caching && client_context) {
		auto context_key = GetContextPrefix() + parsed_url.storage_account_name;

		auto &registered_state = client_context->registered_state;

		result = registered_state->Get<AzureContextState>(context_key);
		if (!result || !result->IsValid()) {
			result = CreateStorageContext(opener, path, parsed_url);
			registered_state->Insert(context_key, result);
		}
	} else {
		result = CreateStorageContext(opener, path, parsed_url);
	}

	return result;
}

AzureReadOptions AzureStorageFileSystem::ParseAzureReadOptions(optional_ptr<FileOpener> opener) {
	AzureReadOptions options;

	Value concurrency_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_transfer_concurrency", concurrency_val)) {
		options.transfer_concurrency = concurrency_val.GetValue<int32_t>();
	}

	Value chunk_size_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_transfer_chunk_size", chunk_size_val)) {
		options.transfer_chunk_size = chunk_size_val.GetValue<int64_t>();
	}

	Value buffer_size_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_buffer_size", buffer_size_val)) {
		options.buffer_size = buffer_size_val.GetValue<idx_t>();
	}

	return options;
}

time_t AzureStorageFileSystem::ToTimeT(const Azure::DateTime &dt) {
	auto time_point = static_cast<std::chrono::system_clock::time_point>(dt);
	return std::chrono::system_clock::to_time_t(time_point);
}

void AzureStorageFileSystem::FileSync(FileHandle &handle) {
	auto &hfh = handle.Cast<AzureFileHandle>();
	if (!hfh.upload_finalized) {
		FlushAllBuffers(hfh);
		FinalizeMultipartUpload(hfh);
	}
}

int64_t AzureStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &afh = handle.Cast<AzureFileHandle>();
	Write(handle, buffer, nr_bytes, afh.file_offset);
	return nr_bytes;
}


void AzureStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<AzureFileHandle>();
	if (!hfh.flags.OpenForWriting()) {
		throw InternalException("Write called on file not opened in write mode");
	}
	int64_t bytes_written = 0;

	while (bytes_written < nr_bytes) {
		auto curr_location = location + bytes_written;

		if (curr_location != hfh.file_offset) {
			throw InternalException("Non-sequential write not supported!");
		}

		// Find buffer for writing
		auto write_buffer_idx = curr_location / hfh.part_size;

		// Get write buffer, may block until buffer is available
		auto write_buffer = hfh.GetBuffer(write_buffer_idx);

		// Writing to buffer
		auto idx_to_write = curr_location - write_buffer->buffer_start;
		auto bytes_to_write = MinValue<idx_t>(nr_bytes - bytes_written, hfh.part_size - idx_to_write);
		memcpy((char *)write_buffer->Ptr() + idx_to_write, (char *)buffer + bytes_written, bytes_to_write);
		write_buffer->idx += bytes_to_write;

		// Flush to HTTP if full
		if (write_buffer->idx >= hfh.part_size) {
			FlushBuffer(hfh, write_buffer);
		}
		hfh.file_offset += bytes_to_write;
		bytes_written += bytes_to_write;
	}
}

// Wrapper around the BufferManager::Allocate to that allows limiting the number of buffers that will be handed out
BufferHandle AzureStorageFileSystem::Allocate(idx_t part_size, uint16_t max_threads) {
	return buffer_manager.Allocate(MemoryTag::EXTENSION, part_size);
}

shared_ptr<AzureWriteBuffer> AzureFileHandle::GetBuffer(uint16_t write_buffer_idx) {
	auto &azurefs = (AzureStorageFileSystem &)file_system;

	// Check if write buffer already exists
	{
		unique_lock<mutex> lck(write_buffers_lock);
		auto lookup_result = write_buffers.find(write_buffer_idx);
		if (lookup_result != write_buffers.end()) {
			shared_ptr<AzureWriteBuffer> buffer = lookup_result->second;
			return buffer;
		}
	}

	auto buffer_handle = azurefs.Allocate(part_size, DEFAULT_MAX_UPLOAD_THREADS);
	auto new_write_buffer =
	    make_shared_ptr<AzureWriteBuffer>(write_buffer_idx * part_size, part_size, std::move(buffer_handle));
	{
		unique_lock<mutex> lck(write_buffers_lock);
		auto lookup_result = write_buffers.find(write_buffer_idx);

		// Check if other thread has created the same buffer, if so we return theirs and drop ours.
		if (lookup_result != write_buffers.end()) {
			// write_buffer_idx << std::endl;
			shared_ptr<AzureWriteBuffer> write_buffer = lookup_result->second;
			return write_buffer;
		}
		write_buffers.insert(pair<uint16_t, shared_ptr<AzureWriteBuffer>>(write_buffer_idx, new_write_buffer));
	}

	return new_write_buffer;
}

void AzureStorageFileSystem::NotifyUploadsInProgress(AzureFileHandle &file_handle) {
	{
		unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
		file_handle.uploads_in_progress--;
	}
	// Note that there are 2 cv's because otherwise we might deadlock when the final flushing thread is notified while
	// another thread is still waiting for an upload thread
	file_handle.uploads_in_progress_cv.notify_one();
	file_handle.final_flush_cv.notify_one();
}

void AzureStorageFileSystem::FlushBuffer(AzureFileHandle &file_handle, shared_ptr<AzureWriteBuffer> write_buffer) {
	if (write_buffer->idx == 0) {
		return;
	}

	auto uploading = write_buffer->uploading.load();
	if (uploading) {
		return;
	}
	bool can_upload = write_buffer->uploading.compare_exchange_strong(uploading, true);
	if (!can_upload) {
		return;
	}

	file_handle.RethrowIOError();

	{
		unique_lock<mutex> lck(file_handle.write_buffers_lock);
		file_handle.write_buffers.erase(write_buffer->part_no);
	}

	{
		unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
		// check if there are upload threads available
		if (file_handle.uploads_in_progress >= DEFAULT_MAX_UPLOAD_THREADS) {
			// there are not - wait for one to become available
			file_handle.uploads_in_progress_cv.wait(lck, [&file_handle] {
				return file_handle.uploads_in_progress < DEFAULT_MAX_UPLOAD_THREADS;
			});
		}
		file_handle.uploads_in_progress++;
	}

	thread upload_thread(UploadBuffer, std::ref(file_handle), write_buffer);
	upload_thread.detach();
}

void AzureFileHandle::Close() {
	auto &afs = (AzureStorageFileSystem &)file_system;
	if (flags.OpenForWriting() && !upload_finalized) {
		afs.FlushAllBuffers(*this);
		if (parts_uploaded) {
			afs.FinalizeMultipartUpload(*this);
		}
	}
}

// Note that FlushAll currently does not allow to continue writing afterwards. Therefore, FinalizeMultipartUpload should
// be called right after it!
// TODO: we can fix this by keeping the last partially written buffer in memory and allow reuploading it with new data.
void AzureStorageFileSystem::FlushAllBuffers(AzureFileHandle &file_handle) {
	//  Collect references to all buffers to check
	vector<shared_ptr<AzureWriteBuffer>> to_flush;
	file_handle.write_buffers_lock.lock();
	for (auto &item : file_handle.write_buffers) {
		to_flush.push_back(item.second);
	}
	file_handle.write_buffers_lock.unlock();

	// Flush all buffers that aren't already uploading
	for (auto &write_buffer : to_flush) {
		if (!write_buffer->uploading) {
			FlushBuffer(file_handle, write_buffer);
		}
	}
	unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
	file_handle.final_flush_cv.wait(lck, [&file_handle] { return file_handle.uploads_in_progress == 0; });

	file_handle.RethrowIOError();
}



} // namespace duckdb
