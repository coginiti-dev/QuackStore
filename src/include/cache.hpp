#pragma once

#include <duckdb.hpp>

#include "block_manager.hpp"
#include "metadata_manager.hpp"

namespace cachefs {

class Cache {
public:
    Cache(uint64_t block_size, 
        duckdb::unique_ptr<BlockManager> block_mg = nullptr, 
        duckdb::unique_ptr<MetadataManager> metadata_mgr = nullptr);
    ~Cache();

    bool IsOpen() const;
    void Open(const duckdb::string &path);
    void Close();

    void Clear();
    void Evict(const duckdb::string& filepath);

    void StoreBlock(const duckdb::string &file_path, int64_t block_index, duckdb::vector<uint8_t> &data);
    bool RetrieveBlock(const duckdb::string &file_path, int64_t block_index, duckdb::vector<uint8_t> &data);

    void StoreFileSize(const duckdb::string &file_path, int64_t file_size);
    void StoreFileLastModified(const duckdb::string &file_path, time_t timestamp);
    bool RetrieveFileMetadata(const duckdb::string &file_path, cachefs::MetadataManager::FileMetadata &file_metadata_out);

    //! Set new max cache size. Triggers eviction if new cache size is less than previous one.
    void SetMaxCacheSize(uint64_t new_max_cache_size_in_bytes);

    //! Flush all changes to disk.
    void Flush();

    uint64_t GetBlockSize() const { return block_size; }
    const duckdb::string& GetPath() const { return path; }

    void AddRef();
    void RemoveRef();

private:
    void Initialize();

    void SetDirty(bool dirty);
    bool IsDirty() const;

private:
    mutable std::recursive_mutex cache_mutex;
    uint64_t block_size = 0;
    uint64_t dirty = 0;
    duckdb::string path;
    bool opened = false;

    duckdb::unique_ptr<BlockManager> block_mgr;
    duckdb::unique_ptr<MetadataManager> metadata_mgr;

    std::atomic<int64_t> current_cache_users = 0;
};

}  // namespace cachefs
