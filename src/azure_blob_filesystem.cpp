#include "azure_blob_filesystem.hpp"

#include "azure_storage_account_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "azure_http_state.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
// TODO: change back to string_common for v1.2.0
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include <azure/storage/blobs.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <thread>

namespace duckdb {

const string AzureBlobStorageFileSystem::SCHEME = "azure";
const string AzureBlobStorageFileSystem::SHORT_SCHEME = "az";

const string AzureBlobStorageFileSystem::PATH_PREFIX = "azure://";
const string AzureBlobStorageFileSystem::SHORT_PATH_PREFIX = "az://";

// taken from s3fs.cpp TODO: deduplicate!
static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		// TODO: change back for v1.2.0
		//if (!Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
		if (!LikeFun::Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

//////// AzureBlobContextState ////////
AzureBlobContextState::AzureBlobContextState(Azure::Storage::Blobs::BlobServiceClient client,
                                             const AzureReadOptions &azure_read_options)
    : AzureContextState(azure_read_options), service_client(std::move(client)) {
}

Azure::Storage::Blobs::BlobContainerClient
AzureBlobContextState::GetBlobContainerClient(const std::string &blobContainerName) const {
	return service_client.GetBlobContainerClient(blobContainerName);
}

//////// AzureBlobStorageFileHandle ////////
AzureBlobStorageFileHandle::AzureBlobStorageFileHandle(AzureBlobStorageFileSystem &fs, string path, FileOpenFlags flags,
                                                       const AzureReadOptions &read_options,
                                                       Azure::Storage::Blobs::BlockBlobClient blob_client)
    : AzureFileHandle(fs, std::move(path), flags, read_options), blob_client(std::move(blob_client)) {
}

//////// AzureBlobStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureBlobStorageFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	if (!opener) {
		throw InternalException("Cannot do Azure storage CreateHandle without FileOpener");
	}

	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	auto parsed_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, parsed_url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(parsed_url.container);
	auto blob_client = container.GetBlockBlobClient(parsed_url.path);

	auto handle = make_uniq<AzureBlobStorageFileHandle>(*this, path, flags, storage_context->read_options,
	                                                    std::move(blob_client));
	if (!handle->PostConstruct()) {
		return nullptr;
	}

	if (flags.OpenForWriting()) {
		auto azure_recommended_part_size = 8*1024*1024;

		// Round part size up to multiple of Storage::DEFAULT_BLOCK_SIZE
		handle->part_size = ((azure_recommended_part_size + Storage::DEFAULT_BLOCK_SIZE - 1) / Storage::DEFAULT_BLOCK_SIZE) *
							Storage::DEFAULT_BLOCK_SIZE;
	}
	return std::move(handle);
}

bool AzureBlobStorageFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind(PATH_PREFIX, 0) * fpath.rfind(SHORT_PATH_PREFIX, 0) == 0;
}

vector<string> AzureBlobStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, azure_url);

	// Azure matches on prefix, not glob pattern, so we take a substring until the first wildcard
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	string shared_path = azure_url.path.substr(0, first_wildcard_pos);
	auto container_client = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(azure_url.container);

	const auto pattern_splits = StringUtil::Split(azure_url.path, "/");
	vector<string> result;

	Azure::Storage::Blobs::ListBlobsOptions options;
	options.Prefix = shared_path;

	const auto path_result_prefix =
	    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' + azure_url.endpoint +
	                                     '/' + azure_url.container)
	                                  : (azure_url.prefix + azure_url.container));
	while (true) {
		// Perform query
		Azure::Storage::Blobs::ListBlobsPagedResponse res;
		try {
			res = container_client.ListBlobs(options);
		} catch (Azure::Storage::StorageException &e) {
			throw IOException("AzureStorageFileSystem Read to %s failed with %s Reason Phrase: %s", path, e.ErrorCode,
			                  e.ReasonPhrase);
		}

		// Assuming that in the majority of the case it's wildcard
		result.reserve(result.size() + res.Blobs.size());

		// Ensure that the retrieved element match the expected pattern
		for (const auto &key : res.Blobs) {
			vector<string> key_splits = StringUtil::Split(key.Name, "/");
			bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

			if (is_match) {
				auto result_full_url = path_result_prefix + '/' + key.Name;
				result.push_back(result_full_url);
			}
		}

		// Manage Azure pagination
		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}

	return result;
}

void AzureBlobStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &hfh = handle.Cast<AzureBlobStorageFileHandle>();

	auto res = hfh.blob_client.GetProperties();
	hfh.length = res.Value.BlobSize;
	hfh.last_modified = ToTimeT(res.Value.LastModified);
}

bool AzureBlobStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener);
		auto &sfh = handle->Cast<AzureBlobStorageFileHandle>();
		if (sfh.length == 0) {
			return false;
		}
		return true;
	} catch (...) {
		return false;
	};
}

void AzureBlobStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                           idx_t buffer_out_len) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();

	try {
		// Specify the range
		Azure::Core::Http::HttpRange range;
		range.Offset = (int64_t)file_offset;
		range.Length = buffer_out_len;
		Azure::Storage::Blobs::DownloadBlobToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.blob_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

shared_ptr<AzureContextState> AzureBlobStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                               const string &path,
                                                                               const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);

	return make_shared_ptr<AzureBlobContextState>(ConnectToBlobStorageAccount(opener, path, parsed_url),
	                                              azure_read_options);
}


/*
 * Base64Encode base64 encodes the input string.
 */
static string
Base64Encode(string input_string)
{
	idx_t base64_length = Blob::ToBase64Size(input_string);
	unique_ptr<char[]> buffer(new char [base64_length + 1]);

	string_t duckdb_string(input_string);
	Blob::ToBase64(duckdb_string, buffer.get());
	buffer[base64_length] = '\0';

	return string(buffer.get());
}


void AzureStorageFileSystem::UploadBuffer(AzureFileHandle &file_handle, shared_ptr<AzureWriteBuffer> write_buffer) {
	auto &afh = file_handle.Cast<AzureBlobStorageFileHandle>();
    auto block_data = Azure::Core::IO::MemoryBodyStream((const uint8_t*)write_buffer->Ptr(), write_buffer->idx);

    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << (write_buffer->part_no + 1);  // e.g., "000001"

	string block_id_base64_string = Base64Encode(oss.str());

	try {
		afh.blob_client.StageBlock(block_id_base64_string, block_data);
	} catch (std::exception &e) {
		// Ensure only one thread sets the exception
		bool f = false;
		auto exchanged = afh.uploader_has_error.compare_exchange_strong(f, true);
		if (exchanged) {
			afh.upload_exception = std::current_exception();
		}

		NotifyUploadsInProgress(file_handle);
		return;
	}

	{
		unique_lock<mutex> lck(file_handle.block_ids_lock);
		file_handle.block_ids.push_back(oss.str());
	}

	file_handle.parts_uploaded++;

	// Free up space for another thread to acquire an AzureWriteBuffer
	write_buffer.reset();

	NotifyUploadsInProgress(file_handle);
}


void AzureStorageFileSystem::FinalizeMultipartUpload(AzureFileHandle &file_handle) {
	auto &afh = file_handle.Cast<AzureBlobStorageFileHandle>();

	std::sort(afh.block_ids.begin(), afh.block_ids.end());

	vector<string> base64_block_ids;

	for (auto &block_id: afh.block_ids)
		base64_block_ids.push_back(Base64Encode(block_id));

    afh.blob_client.CommitBlockList(base64_block_ids);

	file_handle.upload_finalized = true;
}


} // namespace duckdb
