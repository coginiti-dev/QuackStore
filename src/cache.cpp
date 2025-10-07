#include <duckdb/common/checksum.hpp>

#include "cache.hpp"

namespace
{
    int64_t NumBlocksFromSize(int64_t cache_size_in_bytes, int64_t block_size) {
        return (cache_size_in_bytes + block_size - 1) / block_size;
    }
}

namespace quackstore {

Cache::Cache(uint64_t block_size, duckdb::unique_ptr<BlockManager> block_manager,
             duckdb::unique_ptr<MetadataManager> metadata_manager)
    : block_size(block_size)
    , block_mgr(block_manager ? std::move(block_manager) : duckdb::make_uniq<BlockManager>(BlockManagerOptions{block_size}))
    , metadata_mgr(metadata_manager ? std::move(metadata_manager) : duckdb::make_uniq<MetadataManager>())
{
    D_ASSERT(!opened);
    D_ASSERT(block_mgr);
    D_ASSERT(metadata_mgr);
}

Cache::~Cache() {
    Close(); 
}

bool Cache::IsOpen() const { 
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};    
    return opened; 
}

void Cache::Open(const duckdb::string &open_path) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    if (IsOpen()) {
        return;
    }

    if (open_path.empty())
    {
        throw duckdb::InvalidInputException("Cache path can't be empty");
    }

    BlockManager::LoadResult load_result = BlockManager::LoadResult::NA;
    auto header = block_mgr->LoadOrCreateDatabase(open_path, &load_result);
    if (load_result == BlockManager::LoadResult::LOADED_EXISTING)
    {
        MetadataReader reader(*block_mgr, block_mgr->GetMetaBlockID());
        metadata_mgr->ReadMetadata(reader, header.version);
    }

    path = open_path;
    opened = true;
    SetDirty(true);
}

void Cache::Close() {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};
    if (!IsOpen()) {
        return;
    }
    if (current_cache_users.load(std::memory_order_acquire) != 0) {
        throw duckdb::IOException("Query cache is in use, please wait for the running queries to finish and try again.");
    }
    Flush();
    if (block_mgr) {
        block_mgr->Close();
        metadata_mgr->Clear();
    }
    opened = false;
    path.clear();
    SetDirty(false);
}

void Cache::Clear() {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};
    if (opened) {
        if (current_cache_users.load(std::memory_order_acquire) != 0) {
            throw duckdb::IOException("Query cache is in use, please wait for the running queries to finish and try again.");
        }
        block_mgr->Clear();
        metadata_mgr->Clear();
        opened = false;
    }

    SetDirty(false);
}

void Cache::Evict(const duckdb::string& filepath)
{
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    MetadataManager::FileMetadata md;
    if (!RetrieveFileMetadata(filepath, md)) return;

    bool evicted = false;
    for(const auto& [block_id, _]: md.blocks)
    {
        metadata_mgr->UnregisterBlock(block_id);
        block_mgr->MarkBlockAsFree(block_id);
        evicted = true;
    }
    // Only mark dirty if something was actually evicted
    SetDirty(evicted);
}

void Cache::Flush() {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    if (!IsOpen() || !IsDirty() || !block_mgr)
    {
        return;
    }

    // Deallocate chained metadata blocks
    auto dealloc_block_id = MetadataReader(*block_mgr, block_mgr->GetMetaBlockID()).GetNextBlockId();
    block_mgr->MarkChainedBlocksAsFree(dealloc_block_id);

    if (metadata_mgr) {
        MetadataWriter writer(*block_mgr, block_mgr->GetMetaBlockID());
        metadata_mgr->WriteMetadata(writer);
    }

    block_mgr->Flush();
    SetDirty(false);
}

void Cache::StoreBlock(const duckdb::string &file_path, int64_t block_index, duckdb::vector<uint8_t> &data) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    uint64_t checksum = duckdb::Checksum(data.data(), data.size());

    block_id_t block_id = metadata_mgr->GetBlockId(file_path, block_index);

    // Allocate new block for the data if it's this is a new data
    if (block_id == BlockManager::INVALID_BLOCK_ID) {
        block_id = block_mgr->AllocBlock();
        metadata_mgr->RegisterBlock(file_path, block_index, block_id, checksum);

        // Evict LRU block if needed
        metadata_mgr->EvictLRUBlockIfNeeded([&](block_id_t block_id) { block_mgr->MarkBlockAsFree(block_id); });
    }

    metadata_mgr->UpdateLRUOrder(block_id);
    block_mgr->StoreBlock(block_id, data);

    SetDirty(true);
}

bool Cache::RetrieveBlock(const duckdb::string &file_path, int64_t block_index, duckdb::vector<uint8_t> &data) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    auto block_id = metadata_mgr->GetBlockId(file_path, block_index);
    if (block_id == BlockManager::INVALID_BLOCK_ID) {
        return false;
    }

    auto block_info = metadata_mgr->GetBlockInfo(file_path, block_id);
    metadata_mgr->UpdateLRUOrder(block_id);
    block_mgr->RetrieveBlock(block_id, data);

    // Verify checksum
    uint64_t computed_checksum = duckdb::Checksum(data.data(), data.size());
    if (block_info.checksum != computed_checksum) {
        // Given block is corrupted, or we got an inconsistent state where metadata and block data is unsync.
        // In this case we mark given block as free and unregister it from the metadata.
        block_mgr->MarkBlockAsFree(block_id);
        metadata_mgr->UnregisterBlock(block_id);

        SetDirty(true);
        return false;
    }

    SetDirty(true);
    return true;
}

void Cache::StoreFileSize(const duckdb::string &file_path, int64_t file_size) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};
    metadata_mgr->SetFileSize(file_path, file_size);
    SetDirty(true);
}

void Cache::StoreFileLastModified(const duckdb::string &file_path, duckdb::timestamp_t timestamp) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};
    metadata_mgr->SetFileLastModified(file_path, timestamp);
    SetDirty(true);
}

bool Cache::RetrieveFileMetadata(const duckdb::string &file_path, MetadataManager::FileMetadata &file_metadata_out) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    return metadata_mgr->GetFileMetadata(file_path, file_metadata_out);
}

void Cache::SetMaxCacheSize(uint64_t new_max_cache_size_in_bytes) {
    duckdb::lock_guard<std::recursive_mutex> lock{cache_mutex};

    auto max_cache_size_in_blocks = NumBlocksFromSize(new_max_cache_size_in_bytes, block_size);

    metadata_mgr->SetMaxCacheSize(max_cache_size_in_blocks);
    metadata_mgr->EvictLRUBlockIfNeeded([&](block_id_t block_id) { block_mgr->MarkBlockAsFree(block_id); });
    SetDirty(true);
}

void Cache::AddRef() {
    current_cache_users.fetch_add(1, std::memory_order_acq_rel);
};
void Cache::RemoveRef() {
    current_cache_users.fetch_sub(1, std::memory_order_acq_rel);
};

bool Cache::IsDirty() const { 
    return dirty; 
}

void Cache::SetDirty(bool flag) {
    if (flag) {
        ++dirty;
        return;
    }
    dirty = 0;
}

}  // namespace quackstore
