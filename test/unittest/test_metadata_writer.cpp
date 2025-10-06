#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/common/file_opener.hpp>

#include "block_manager.hpp"
#include "metadata_reader.hpp"
#include "metadata_writer.hpp"

using namespace quackstore;

TEST_CASE("MetadataWriter handles multiple block writes correctly", "[MetadataWriter]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    BlockManagerOptions options;
    options.block_size = Kilobytes(1);

    SECTION("Write and read single block of data") {
        std::string test_data = "This is some test metadata data.";

        std::vector<block_id_t> expected_blocks;
        {
            auto block_mgr = BlockManager{options};
            block_mgr.CreateNewDatabase(storage_file_path);

            MetadataWriter writer(block_mgr, block_mgr.GetMetaBlockID());

            writer.WriteData(reinterpret_cast<const uint8_t *>(test_data.data()), test_data.size());
            writer.Flush();
            block_mgr.Flush();

            // Capture expected blocks used
            expected_blocks = writer.GetUsedMetadataBlocks();
            REQUIRE(expected_blocks.size() == 1);
        }

        {
            auto block_mgr = BlockManager{options};
            block_mgr.LoadExistingDatabase(storage_file_path);

            // Create a reader and read back the data
            MetadataReader reader(block_mgr, block_mgr.GetMetaBlockID());
            std::vector<uint8_t> read_data(test_data.size(), 0);
            reader.ReadData(read_data.data(), read_data.size());

            // Assert that the read data matches the written data
            REQUIRE(test_data == std::string(read_data.begin(), read_data.end()));

            // Verify the blocks used during read match the expected blocks
            auto used_blocks = reader.GetUsedMetadataBlocks();
            REQUIRE(used_blocks == expected_blocks);
        }
    }

    SECTION("Write and read data spanning multiple blocks") {
        // Generate test data larger than a single block
        auto block_size = options.block_size;
        std::string test_data(block_size * 3, 'x');  // 3 full blocks worth of data filled with 'x' will give us 4
                                                     // blocks in total, since we need 8 additional bytes per block to
                                                     // store next_block_id pointer

        std::vector<block_id_t> expected_blocks;

        // Write test data
        {
            auto block_mgr = BlockManager{options};
            block_mgr.CreateNewDatabase(storage_file_path);

            MetadataWriter writer(block_mgr, block_mgr.GetMetaBlockID());

            writer.WriteData(reinterpret_cast<const uint8_t *>(test_data.data()), test_data.size());
            writer.Flush();
            block_mgr.Flush();

            // Capture expected blocks used
            expected_blocks = writer.GetUsedMetadataBlocks();
            REQUIRE(expected_blocks.size() == 4);  // Expect 4 blocks for the data
        }

        // Read and verify test data
        {
            auto block_mgr = BlockManager{options};
            block_mgr.LoadExistingDatabase(storage_file_path);

            MetadataReader reader(block_mgr, block_mgr.GetMetaBlockID());

            // Buffer to hold the read data
            std::vector<uint8_t> read_data(test_data.size(), 0);
            reader.ReadData(read_data.data(), read_data.size());

            // Verify the read data matches what was written
            REQUIRE(test_data == std::string(read_data.begin(), read_data.end()));

            // Verify the blocks used during read match the expected blocks
            auto used_blocks = reader.GetUsedMetadataBlocks();
            REQUIRE(used_blocks == expected_blocks);
        }
    }
}
