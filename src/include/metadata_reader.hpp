#pragma once

#include <duckdb.hpp>
#include <duckdb/common/serializer/read_stream.hpp>

namespace cachefs {

class BlockManager;

class MetadataReader : public duckdb::ReadStream {
public:
    explicit MetadataReader(BlockManager &block_mgr);
    MetadataReader(BlockManager &block_mgr, block_id_t start_block_id);

    void ReadData(duckdb::data_ptr_t buffer, idx_t read_size) override;

    //! Get the list of blocks used during this read operation
    const duckdb::vector<block_id_t> &GetUsedMetadataBlocks() const { return used_metadata_blocks; }
    block_id_t GetNextBlockId() const;

    bool ReadBlock(block_id_t id);

private:
    void Reset();
    void SetNextBlockId(block_id_t id);

private:
    BlockManager &block_mgr;
    idx_t offset;
    duckdb::vector<uint8_t> current_block_data;

    // List of block IDs used during the read operation
    duckdb::vector<block_id_t> used_metadata_blocks;
};

}  // namespace cachefs
