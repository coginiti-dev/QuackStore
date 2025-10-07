#include "block_manager.hpp"
#include "metadata_writer.hpp"

namespace quackstore {

MetadataWriter::MetadataWriter(BlockManager &block_mgr, block_id_t start_block_id)
    : block_mgr(block_mgr)
    , current_block_id(start_block_id)
    , offset(sizeof(block_id_t))
    , current_block_data(block_mgr.GetBlockSize(), 0xFF)
{
    if (start_block_id == BlockManager::INVALID_BLOCK_ID) {
        throw duckdb::InvalidInputException("Invalid block ID provided to MetadataWriter");
    }
    SetNextBlockId(BlockManager::INVALID_BLOCK_ID);
    used_metadata_blocks.push_back(start_block_id);
}

MetadataWriter::~MetadataWriter() { 
    // Set next block ID as INVALID to signify end
    SetNextBlockId(BlockManager::INVALID_BLOCK_ID);
    Flush();
}

void MetadataWriter::WriteData(duckdb::const_data_ptr_t buffer, idx_t write_size) {
    idx_t bytes_written = 0;

    while (bytes_written < write_size) {
        idx_t space_left = block_mgr.GetBlockSize() - offset;

        if (space_left == 0) {
            AllocateNewBlock();
            space_left = block_mgr.GetBlockSize() - offset;
        }

        idx_t chunk_size = std::min(write_size - bytes_written, space_left);
        std::memcpy(&current_block_data[offset], buffer + bytes_written, chunk_size);

        bytes_written += chunk_size;
        offset += chunk_size;
    }
}

void MetadataWriter::Flush() {
    block_mgr.StoreBlock(current_block_id, current_block_data);
}

void MetadataWriter::AllocateNewBlock() {
    block_id_t next_block_id = block_mgr.AllocBlock();
    if (current_block_id != BlockManager::INVALID_BLOCK_ID) {
        SetNextBlockId(next_block_id);
        Flush();
    }

    Reset();
    current_block_id = next_block_id;
    SetNextBlockId(BlockManager::INVALID_BLOCK_ID);
    // Add current block to used metadata block list
    used_metadata_blocks.push_back(current_block_id);
}

void MetadataWriter::SetNextBlockId(block_id_t id) {
    std::memcpy(&current_block_data[0], &id, sizeof(id));
}

void MetadataWriter::Reset()
{
    offset = sizeof(block_id_t);
    std::fill(current_block_data.begin(), current_block_data.end(), 0xFF);
}

}  // namespace quackstore
