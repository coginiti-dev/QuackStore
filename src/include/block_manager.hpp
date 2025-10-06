#pragma once

#include <duckdb.hpp>

namespace cachefs {

#define Bytes(n) (n)
#define Kilobytes(n) (n << 10)
#define Megabytes(n) (n << 20)
#define Gigabytes(n) (((uint64_t)n) << 30)

using block_id_t = int64_t;

// =============================================================================
// BlockCacheDataFileHeader
// =============================================================================

struct BlockCacheDataFileHeader {
    uint32_t version;
    //! A pointer to the initial block containing metadata
    block_id_t meta_block;
    //! A pointer to the block containing the free list
    block_id_t free_list;
    //! The number of blocks in the storage.
    uint64_t block_count;
    //! The block_size.
    uint64_t block_size;

    void Write(duckdb::WriteStream &ser);
    static BlockCacheDataFileHeader Read(duckdb::ReadStream &source);
    static size_t Size();
};

// =============================================================================
// BlockManager
// =============================================================================

struct BlockManagerOptions {
    uint64_t block_size;
};

class BlockManager {
    static constexpr idx_t FILE_HEADER_SIZE = 4096U;
    //! The location in the file where the block writing starts.
    static constexpr uint64_t BLOCK_START = FILE_HEADER_SIZE;

public:
    //! Used to indicate an invalid block id.
    constexpr static block_id_t INVALID_BLOCK_ID = -1;

    enum class LoadResult {
        NA = 0,
        LOADED_EXISTING,
        CREATED_NEW
    };

public:
    BlockManager(const BlockManagerOptions &options);
    virtual ~BlockManager();

    void Close();
    bool IsOpen() const {
        return handle != nullptr;
    }
    void Clear();

    BlockCacheDataFileHeader LoadOrCreateDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out = nullptr);
    BlockCacheDataFileHeader CreateNewDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out = nullptr);
    BlockCacheDataFileHeader LoadExistingDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out = nullptr);

    void Flush();

    //! Allocate a new block within the block storage.
    block_id_t AllocBlock();
    virtual void StoreBlock(block_id_t block_id, const duckdb::vector<uint8_t> &data);
    void RetrieveBlock(block_id_t block_id, duckdb::vector<uint8_t> &data);
    void MarkBlockAsFree(block_id_t block_id);
    size_t MarkChainedBlocksAsFree(block_id_t block_id);

    uint64_t GetBlockSize() const;
    block_id_t GetMetaBlockID();

    //! Used only for testing
    const duckdb::set<block_id_t> &GetFreeList() const;
    block_id_t GetMaxBlock() const;

private:
    uint64_t GetBlockOffset(block_id_t block_id);
    void SaveFreeList();
    void LoadFreeList();
    void WriteHeader();
    void ValidateBlockId(block_id_t block_id) const;
    void ValidateHandle() const;
    void CloseHandle();
    void CloseInternal();

private:
    duckdb::mutex block_manager_mutex;
    //! The file system used for the block cache.
    duckdb::unique_ptr<duckdb::FileSystem> fs;
    //! Storage options.
    BlockManagerOptions options;
    //! The file handle to the block cache file.
    duckdb::unique_ptr<duckdb::FileHandle> handle;

    //! The maximum block index that is stored in the file.
    uint64_t max_block;
    //! The block_id where metadata is stored
    block_id_t meta_block_id;
    //! The block_id where free_list is stored
    block_id_t free_list_id;

    //! The free list of block ids.
    duckdb::set<block_id_t> free_list;
};

}  // namespace cachefs
