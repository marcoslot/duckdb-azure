#pragma once

#include "azure_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <azure/core/datetime.hpp>
#include <ctime>
#include <cstdint>
#include <iostream>

namespace duckdb {

struct AzureReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};

class AzureContextState : public ClientContextState {
public:
	const AzureReadOptions read_options;

public:
	virtual bool IsValid() const;
	void QueryEnd() override;

	template <class TARGET>
	TARGET &As() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &As() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	AzureContextState(const AzureReadOptions &read_options);

protected:
	bool is_valid;
};

class AzureStorageFileSystem;

// Holds the buffered data for 1 part of an S3 Multipart upload
class AzureWriteBuffer {
public:
	explicit AzureWriteBuffer(idx_t buffer_start, size_t buffer_size, BufferHandle buffer_p)
	    : idx(0), buffer_start(buffer_start), buffer(std::move(buffer_p)) {
		buffer_end = buffer_start + buffer_size;
		part_no = buffer_start / buffer_size;
		uploading = false;
	}

	void *Ptr() {
		return buffer.Ptr();
	}

	idx_t part_no;

	idx_t idx;
	idx_t buffer_start;
	idx_t buffer_end;
	BufferHandle buffer;
	atomic<bool> uploading;
};


class AzureFileHandle : public FileHandle {
public:
	virtual bool PostConstruct();
	void Close() override;

	~AzureFileHandle() override;

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, string path, FileOpenFlags flags, const AzureReadOptions &read_options);

public:
	FileOpenFlags flags;

	// File info
	idx_t length;
	time_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	size_t part_size;

	//! Write buffers for this file
	mutex write_buffers_lock;
	unordered_map<uint16_t, shared_ptr<AzureWriteBuffer>> write_buffers;

	//! Synchronization for upload threads
	mutex uploads_in_progress_lock;
	std::condition_variable uploads_in_progress_cv;
	std::condition_variable final_flush_cv;
	uint16_t uploads_in_progress;

	//! Etags are stored for each part
	mutex block_ids_lock;
	vector<string> block_ids;

	//! Info for upload
	atomic<uint16_t> parts_uploaded;
	bool upload_finalized = true;

	//! Error handling in upload threads
	atomic<bool> uploader_has_error {false};
	std::exception_ptr upload_exception;

	shared_ptr<AzureWriteBuffer> GetBuffer(uint16_t write_buffer_idx);

	//! Rethrow IO Exception originating from an upload thread
	void RethrowIOError() {
		if (uploader_has_error) {
			std::rethrow_exception(upload_exception);
		}
	}

	const AzureReadOptions read_options;
};

class AzureStorageFileSystem : public FileSystem {
public:
	explicit AzureStorageFileSystem(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	BufferManager &buffer_manager;

	// FS methods
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return false;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	bool LoadFileInfo(AzureFileHandle &handle);

	BufferHandle Allocate(idx_t part_size, uint16_t max_threads);
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void FinalizeMultipartUpload(AzureFileHandle &file_handle);
	void FlushAllBuffers(AzureFileHandle &handle);

	static void UploadBuffer(AzureFileHandle &file_handle, shared_ptr<AzureWriteBuffer> write_buffer);

protected:
	virtual duckdb::unique_ptr<AzureFileHandle> CreateHandle(const string &path, FileOpenFlags flags,
	                                                         optional_ptr<FileOpener> opener) = 0;
	virtual void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) = 0;

	virtual const string &GetContextPrefix() const = 0;
	shared_ptr<AzureContextState> GetOrCreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                        const AzureParsedUrl &parsed_url);
	virtual shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                           const AzureParsedUrl &parsed_url) = 0;

	virtual void LoadRemoteFileInfo(AzureFileHandle &handle) = 0;
	static AzureReadOptions ParseAzureReadOptions(optional_ptr<FileOpener> opener);
	static time_t ToTimeT(const Azure::DateTime &dt);

	static void NotifyUploadsInProgress(AzureFileHandle &file_handle);
	void FlushBuffer(AzureFileHandle &handle, shared_ptr<AzureWriteBuffer> write_buffer);
};

} // namespace duckdb
