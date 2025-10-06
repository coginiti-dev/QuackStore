#pragma once

#include <duckdb/common/list.hpp>

#include "block_manager.hpp"
#include "metadata_reader.hpp"
#include "metadata_writer.hpp"

namespace cachefs {

// =============================================================================
// MetadataManager
// =============================================================================

class MetadataManager {
public:
    struct BlockKey {
        duckdb::string file_path;
        int64_t block_index;

        bool operator==(const BlockKey &other) const {
            return block_index == other.block_index && file_path == other.file_path;
        }
    };

    // Custom hash function for BlockKey
    struct BlockKeyHash {
        size_t operator()(const BlockKey &key) const {
            return std::hash<duckdb::string>()(key.file_path) ^ std::hash<int64_t>()(key.block_index);
        }
    };

    //! Stores block_index from the source data and block_id from the block storage
    struct FileMetadataBlockInfo {
        int64_t block_index;
        block_id_t block_id;
        uint64_t checksum;
    };

    //! FileMetadata tracks file size and list of blocks allocated for the given file
    struct FileMetadata {
        uint64_t file_size = 0;
        duckdb::unordered_map<block_id_t, FileMetadataBlockInfo> blocks;
        time_t last_modified = 0;

        void Write(duckdb::WriteStream &ser) const;
        static FileMetadata Read(duckdb::ReadStream &source, uint32_t version);

        duckdb::string ToString() const;
    private:
        static void ReadV1(duckdb::ReadStream &source, MetadataManager::FileMetadata& out);
        static void ReadV2(duckdb::ReadStream &source, MetadataManager::FileMetadata& out);
    };

    MetadataManager();
    ~MetadataManager();

    void Clear();

    block_id_t GetBlockId(const duckdb::string &file_path, int64_t block_index) const;
    void RegisterBlock(const duckdb::string &file_path, int64_t block_index, block_id_t block_id, uint64_t checksum);
    void UnregisterBlock(block_id_t block_id);
    void SetFileSize(const duckdb::string &file_path, int64_t file_size);
    void SetFileLastModified(const duckdb::string &file_path, time_t timestamp);
    bool GetFileMetadata(const duckdb::string &file_path, FileMetadata &file_metadata_out) const;

    void UpdateLRUOrder(block_id_t block_id);
    void EvictLRUBlockIfNeeded(std::function<void(block_id_t)> remove_from_storage_func);

    void WriteMetadata(MetadataWriter &writer);
    void ReadMetadata(MetadataReader &reader, uint32_t version);

    void SetMaxCacheSize(idx_t max_cache_size_in_blocks);

    FileMetadataBlockInfo GetBlockInfo(const duckdb::string &file_path, block_id_t block_id) const;

    //! Used for testing only
    duckdb::vector<BlockKey> GetLRUState() const;

private:
    //! The mapping of file paths and block indices to block ids.
    duckdb::unordered_map<BlockKey, block_id_t, BlockKeyHash> block_mapping;
    //! Reverse mapping from block_id to BlockKey to easily locate which file/block is associated with a block_id
    duckdb::unordered_map<block_id_t, BlockKey> reverse_block_mapping;
    //! The mapping of file paths and files metadata.
    duckdb::unordered_map<duckdb::string, FileMetadata> files_metadata;

    //! Cache capacity (measured in number of blocks)
    idx_t max_cache_size;
    //! Linked list to store lru order
    duckdb::list<block_id_t> lru_list;
    //! Maps block_id_t to the correspondent node in the linked list `lru_list` to get O(1) access time
    duckdb::unordered_map<block_id_t, duckdb::list<block_id_t>::iterator> lru_map;
};

}  // namespace cachefs
