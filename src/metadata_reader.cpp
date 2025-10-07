#include "block_manager.hpp"
#include "metadata_reader.hpp"

namespace quackstore {

MetadataReader::MetadataReader(BlockManager &block_mgr)
    : block_mgr(block_mgr)
    , offset(sizeof(block_id_t)) 
    , current_block_data(block_mgr.GetBlockSize(), 0xFF)
{
}

MetadataReader::MetadataReader(BlockManager &block_mgr, block_id_t start_block_id)
    : MetadataReader(block_mgr)
{
    ReadBlock(start_block_id);
}

void MetadataReader::ReadData(duckdb::data_ptr_t buffer, idx_t read_size) {
    idx_t bytes_read = 0;

    while (bytes_read < read_size) {
        if (offset >= current_block_data.size()) {
            auto next_block_id = GetNextBlockId();
            if (!ReadBlock(next_block_id))
            {
                break; // No more blocks to read
            }
        }

        idx_t space_left = current_block_data.size() - offset;
        idx_t chunk_size = std::min(read_size - bytes_read, space_left);

        std::memcpy(buffer + bytes_read, &current_block_data[offset], chunk_size);

        bytes_read += chunk_size;
        offset += chunk_size;
    }
}

void MetadataReader::ReadData(duckdb::QueryContext context, duckdb::data_ptr_t buffer, idx_t read_size) {
    ReadData(buffer, read_size);
}

block_id_t MetadataReader::GetNextBlockId() const
{
    block_id_t id = BlockManager::INVALID_BLOCK_ID;
    std::memcpy(&id, &current_block_data[0], sizeof(id));
    return id;
}

bool MetadataReader::ReadBlock(block_id_t id) {
    if (id == BlockManager::INVALID_BLOCK_ID) {
        SetNextBlockId(BlockManager::INVALID_BLOCK_ID);
        offset = current_block_data.size(); // Mark as end of stream
        return false; // No more blocks to read
    }
    block_mgr.RetrieveBlock(id, current_block_data);
    offset = sizeof(block_id_t);
    used_metadata_blocks.push_back(id);
    return true;
}

void MetadataReader::SetNextBlockId(block_id_t id)
{
    std::memcpy(&current_block_data[0], &id, sizeof(id));
}

}  // namespace quackstore
