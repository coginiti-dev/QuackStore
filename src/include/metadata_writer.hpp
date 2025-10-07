#pragma once

#include <duckdb/common/serializer/write_stream.hpp>

namespace quackstore {

class BlockManager;

class MetadataWriter : public duckdb::WriteStream {
public:
    MetadataWriter(BlockManager &block_mgr, block_id_t start_block_id);
    ~MetadataWriter() override;

    void WriteData(duckdb::const_data_ptr_t buffer, idx_t write_size) override;
    void Flush();

    //! Get the list of blocks used during this write operation
    const duckdb::vector<block_id_t> &GetUsedMetadataBlocks() const { return used_metadata_blocks; }
    void SetNextBlockId(block_id_t id);

private:
    void AllocateNewBlock();
    void Reset();

private:
    BlockManager &block_mgr;
    block_id_t current_block_id;
    idx_t offset;
    duckdb::vector<uint8_t> current_block_data;

    // List of block IDs used during the write operation
    duckdb::vector<block_id_t> used_metadata_blocks;
};

}  // namespace quackstore
