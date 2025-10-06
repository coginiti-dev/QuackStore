#include <algorithm>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/interval.hpp>

#include "cache_file_system.hpp"
#include "cache_params.hpp"
#include "cache.hpp"

namespace {
    duckdb::string StripPrefix(const duckdb::string &text, const duckdb::string &prefix) {
        return text.rfind(prefix, 0) == 0 ? text.substr(prefix.length()) : text;
    }
}

namespace cachefs {

// =============================================================================
// BlockCachingFileHandle
// =============================================================================

class CacheFileHandle : public duckdb::FileHandle {
public:
    CacheFileHandle(
        CacheFileSystem &cache_fs, 
        const duckdb::string &path,
        duckdb::FileSystem& underlying_fs,
        Cache& cache,
        ExtensionParams params
    )
    : duckdb::FileHandle(cache_fs, path, duckdb::FileOpenFlags::FILE_FLAGS_READ)
    , underlying_fs(underlying_fs)
    , cache(cache)
    , is_open(true)
    {
        // Lazy getters to avoid unnecessary IO calls
        time_t underlying_last_modified = 0;
        bool underlying_last_modified_requested = false;
        auto get_underlying_last_modified = [&]() {
            if (!underlying_last_modified_requested) {
                underlying_last_modified = GetFileLastModifiedUnderlying();
                underlying_last_modified_requested = true;
            }
            return underlying_last_modified;
        };

        int64_t underlying_filesize = 0;
        bool underlying_filesize_requested = false;
        auto get_underlying_filesize = [&]() {
            if (!underlying_filesize_requested) {
                underlying_filesize = GetFileSizeUnderlying();
                underlying_filesize_requested = true;
            }
            return underlying_filesize;
        };

        cache.AddRef();
        try
        {
            // Check if file metadata exists in cache
            MetadataManager::FileMetadata md;
            if (!cache.RetrieveFileMetadata(path, md))
            {
                // First time caching this file - store metadata
                cache.StoreFileSize(GetPath(), get_underlying_filesize());
                cache.StoreFileLastModified(GetPath(), get_underlying_last_modified());
                return;
            }

            // For mutable data, validate cache freshness
            if (params.data_mutable)
            {
                bool evict_file_entry = false;

                if (md.last_modified != get_underlying_last_modified())
                {
                    evict_file_entry = true;
                }
                else if (underlying_last_modified == 0)
                {
                    // Certain FS don't provide LastModified property. Let's check file size
                    evict_file_entry = (md.file_size != get_underlying_filesize() || get_underlying_filesize() == 0);
                }

                if (evict_file_entry)
                {
                    // File changed - invalidate cache and update metadata
                    cache.Evict(path);

                    time_t last_modified = get_underlying_last_modified();
                    cache.StoreFileLastModified(GetPath(), last_modified);

                    int64_t filesize = get_underlying_filesize();
                    cache.StoreFileSize(GetPath(), filesize);
                }
            }
        }
        catch (...)
        {
            Close();
            throw;
        }
    };

    ~CacheFileHandle() override {
        Close();
    }

public:
    void Close() override {
        if (!is_open)
        {
            return;
        }

        is_open = false;
        if (underlying_file_handle) {
            underlying_file_handle->Close();
        }
        cache.Flush();
        cache.RemoveRef();
    }

    void ReadChunk(void *buffer, int64_t nr_bytes, idx_t location) const {
        ValidateIsOpen();

        current_location = location;
        ReadChunk(buffer, nr_bytes);
    }

    int64_t ReadChunk(void *buffer, int64_t nr_bytes) const {
        ValidateIsOpen();

        int64_t total_bytes_read = 0;
        auto read_buffer = duckdb::char_ptr_cast(buffer);

        // Adjust nr_bytes if it attempts to read beyond EOF
        int64_t file_size = GetFileSize();
        if (current_location + nr_bytes > file_size) {
            nr_bytes = file_size - current_location;
        }

        auto block_size = cache.GetBlockSize();
        duckdb::vector<uint8_t> block_data(block_size);

        while (nr_bytes > 0) {
            idx_t block_index = current_location / block_size;
            idx_t block_offset = current_location % block_size;

            // Calculate the remaining bytes to read in the current block
            idx_t bytes_to_read = std::min(static_cast<idx_t>(nr_bytes), block_size - block_offset);

            // Check if the block is in the cache
            if (!cache.RetrieveBlock(GetPath(), block_index, block_data)) {
                idx_t bytes_left_in_file = file_size - (block_index * block_size);
                idx_t bytes_to_read_from_file = std::min(static_cast<idx_t>(block_size), bytes_left_in_file);

                UnderlyingFileHandle()->Read(block_data.data(), bytes_to_read_from_file, block_index * block_size);

                // Save the block to the cache.
                cache.StoreBlock(GetPath(), block_index, block_data);
            }

            std::copy(block_data.begin() + block_offset, block_data.begin() + block_offset + bytes_to_read,
                      read_buffer);

            read_buffer += bytes_to_read;
            nr_bytes -= bytes_to_read;
            current_location += bytes_to_read;
            total_bytes_read += bytes_to_read;
        }

        return total_bytes_read;
    }

    duckdb::unique_ptr<duckdb::FileHandle>& UnderlyingFileHandle() const {
        ValidateIsOpen();
        if (!underlying_file_handle) {
            auto underlying_path = StripPrefix(path, CacheFileSystem::SCHEMA_PREFIX);
            underlying_file_handle = underlying_fs.OpenFile(underlying_path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        }
        return underlying_file_handle;
    }

    int64_t GetFileSize() const {
        int64_t file_size = 0;
        if (GetFileSizeCached(file_size))
        {
            return file_size;
        }

        file_size = GetFileSizeUnderlying();
        cache.StoreFileSize(GetPath(), file_size);
        return file_size;
    }

    time_t GetFileLastModified() const {
        time_t last_modified = 0;
        if (GetFileLastModifiedCached(last_modified))
        {
            return last_modified;
        }

        last_modified = GetFileLastModifiedUnderlying();
        cache.StoreFileLastModified(GetPath(), last_modified);
        return last_modified;
    }

private:
    bool GetFileSizeCached(int64_t& val) const {
        MetadataManager::FileMetadata md;
        if (cache.RetrieveFileMetadata(GetPath(), md))
        {
            val = md.file_size;
            return true;
        }
        val = 0;
        return false;
    }

    bool GetFileLastModifiedCached(time_t& val) const {
        val = 0;
        MetadataManager::FileMetadata md;
        if (cache.RetrieveFileMetadata(GetPath(), md))
        {
            val = md.last_modified;
            return true;
        }
        val = 0;
        return false;
    }

    int64_t GetFileSizeUnderlying() const
    {
        return underlying_fs.GetFileSize(*UnderlyingFileHandle());
    }

    time_t GetFileLastModifiedUnderlying() const
    {
        return underlying_fs.GetLastModifiedTime(*UnderlyingFileHandle());
    }

    void ValidateIsOpen() const
    {
        if (!is_open) 
        {
            throw duckdb::InternalException("Can't operate on a closed handle");
        }
    }

public:
    mutable int64_t current_location = 0;

private:
    duckdb::FileSystem& underlying_fs;
    mutable duckdb::unique_ptr<duckdb::FileHandle> underlying_file_handle;
    Cache& cache;
    bool is_open = false;
};

// =============================================================================
// BlockCachingFileSystem
// =============================================================================

CacheFileSystem::CacheFileSystem(Cache& cache)
: cache(cache)
{}

duckdb::unique_ptr<duckdb::FileHandle> CacheFileSystem::OpenFile(const duckdb::string &path, duckdb::FileOpenFlags flags,
                                                 duckdb::optional_ptr<duckdb::FileOpener> opener) {
    if (!opener)
    {
        throw duckdb::InvalidInputException("Opener can't be null");
    }

    ExtensionParams params;
    duckdb::FileSystem* underlying_fs_ptr = nullptr;
    auto optional_cc = opener->TryGetClientContext();
    auto optional_db = opener->TryGetDatabase();
    if (optional_cc)
    {
        params = ExtensionParams::ReadFrom(*optional_cc);
        underlying_fs_ptr = &duckdb::FileSystem::GetFileSystem(*optional_cc);
    }
    else if (optional_db)
    {
        params = ExtensionParams::ReadFrom(*optional_db);
        underlying_fs_ptr = &duckdb::FileSystem::GetFileSystem(*optional_db);
    }
    else
    {
        throw duckdb::InvalidInputException("Unable to read CacheFS parameters");
    }

    auto& underlying_fs = *underlying_fs_ptr;
    if (params.cache_enabled == false) {
        auto actual_path = StripPrefix(path, SCHEMA_PREFIX);
        return underlying_fs.OpenFile(actual_path, flags);
    }

    if (!cache.IsOpen()) {
        cache.Open(params.cache_path);
    }

    cache.SetMaxCacheSize(params.max_cache_size);

    return duckdb::make_uniq<CacheFileHandle>(*this, path, underlying_fs, cache, std::move(params));
}

bool CacheFileSystem::CanHandleFile(const duckdb::string &path) {
    return path.rfind(SCHEMA_PREFIX, 0) == 0;
}


void CacheFileSystem::Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    caching_file_handle.ReadChunk(buffer, nr_bytes, location);
}

int64_t CacheFileSystem::Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    return caching_file_handle.ReadChunk(buffer, nr_bytes);
}

duckdb::vector<duckdb::string> CacheFileSystem::Glob(const duckdb::string &path, duckdb::FileOpener *opener) {
    duckdb::string actual_path = StripPrefix(path, SCHEMA_PREFIX);

    duckdb::FileSystem* ufs = nullptr;
    auto optional_cc = opener->TryGetClientContext();
    auto optional_db = opener->TryGetDatabase();
    if (optional_cc)
    {
        ufs = &duckdb::FileSystem::GetFileSystem(*optional_cc);
    }
    else if (optional_db)
    {
        ufs = &duckdb::FileSystem::GetFileSystem(*optional_db);
    }
    else
    {
        throw duckdb::InvalidInputException("Unable to read CacheFS parameters");
    }
    auto& underlying_fs = *ufs;

    auto entries = underlying_fs.Glob(actual_path);
    if (path.rfind(SCHEMA_PREFIX, 0) == 0) {
        for (auto &e : entries) {
            e = SCHEMA_PREFIX + e;
        }
    }

    return entries;
}

int64_t CacheFileSystem::GetFileSize(duckdb::FileHandle &handle) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    return caching_file_handle.GetFileSize();
}

void CacheFileSystem::Seek(duckdb::FileHandle &handle, idx_t location) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    caching_file_handle.current_location = location;
}

idx_t CacheFileSystem::SeekPosition(duckdb::FileHandle &handle) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    return caching_file_handle.current_location;
}

time_t CacheFileSystem::GetLastModifiedTime(duckdb::FileHandle &handle) {
    auto &caching_file_handle = handle.Cast<CacheFileHandle>();
    return caching_file_handle.GetFileLastModified();
}

}  // namespace cachefs
