#include <catch/catch.hpp>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/file_opener.hpp>
#include <random>

#include "cache.hpp"

using namespace quackstore;

namespace Catch {
// A specialization for `BlockKey` string conversion
template <> struct StringMaker<MetadataManager::BlockKey> {
    static std::string convert(const MetadataManager::BlockKey& key) {
        return "{file_path: \"" + key.file_path + "\", block_index: " + std::to_string(key.block_index) + "}";
    }
};
}  // namespace Catch

TEST_CASE("Make sure cache uses the correct path", "[Cache]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    REQUIRE_FALSE(local_fs->FileExists(storage_file_path));
    Cache cache(Kilobytes(64));
    cache.Open(storage_file_path);
    REQUIRE(cache.IsOpen());
    REQUIRE(local_fs->FileExists(storage_file_path));
}

TEST_CASE("LRU eviction", "[Cache]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    uint64_t block_size = Megabytes(1);
    int64_t num_blocks = 10;

    duckdb::unique_ptr<Cache> cache = duckdb::make_uniq<Cache>(block_size);
    cache->Open(storage_file_path);
    cache->SetMaxCacheSize(num_blocks * block_size);

    duckdb::vector<uint8_t> block_data(block_size, 'b');
    duckdb::string file_path = "/test/lru_block";

    SECTION("Evict oldest block after cache capacity is reached") {
        for (int64_t i = 0; i < num_blocks + 10; ++i) {  // Adding 10 more than capacity
            cache->StoreBlock(file_path, i, block_data);
        }

        duckdb::vector<uint8_t> dummy_data(block_size, 0);
        REQUIRE_FALSE(cache->RetrieveBlock(file_path, 0, dummy_data));  // Oldest should be evicted

        REQUIRE(cache->RetrieveBlock(file_path, 10, dummy_data));  // Should be available
    }
}

TEST_CASE("Restores correct LRU order on load", "[Cache]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    uint64_t block_size = Megabytes(1);
    int64_t num_blocks = 5;
    duckdb::string file_path = "https://test/load_lru_block.parquet";

    // Initialize cache
    {
        duckdb::unique_ptr<Cache> cache = duckdb::make_uniq<Cache>(block_size);
        cache->Open(storage_file_path);
        cache->SetMaxCacheSize(num_blocks * block_size);

        duckdb::vector<uint8_t> block_data(block_size, 'd');  // Dummy data for testing

        // Insert blocks and simulate access in a specific order
        for (int64_t i = 0; i < num_blocks; ++i) {
            cache->StoreBlock(file_path, i, block_data);
        }

        // Simulate some access patterns
        cache->RetrieveBlock(file_path, 1, block_data);
        cache->RetrieveBlock(file_path, 3, block_data);
        cache->RetrieveBlock(file_path, 4, block_data);
    }

    // Reinitialize cache
    {
        auto metadata_manager_ptr = duckdb::make_uniq<MetadataManager>();
        auto& metadata_manager = *metadata_manager_ptr;
        duckdb::unique_ptr<Cache> cache = duckdb::make_uniq<Cache>(block_size, 
            nullptr, 
            std::move(metadata_manager_ptr));
        cache->Open(storage_file_path);
        cache->SetMaxCacheSize(num_blocks * block_size);

        SECTION("LRU order after reload") {
            // Check that the LRU order reflects our access pattern:
            duckdb::vector<MetadataManager::BlockKey> expected_lru_order = {
                {file_path, 4}, {file_path, 3}, {file_path, 1}, {file_path, 2}, {file_path, 0}};

            // Simulate the functionality internally checking LRU from back (least recent) to front (most recent)
            auto current_lru_state = metadata_manager.GetLRUState();

            REQUIRE(current_lru_state.size() == expected_lru_order.size());

            for (size_t i = 0; i < expected_lru_order.size(); ++i) {
                INFO("Checking LRU state at index: " << i);
                REQUIRE(current_lru_state[i] == expected_lru_order[i]);
            }
        }
    }
}

TEST_CASE("SetMaxCacheSize triggers eviction when reducing max cache size", "[Cache]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    uint64_t block_size = Megabytes(1);
    int64_t initial_num_blocks = 10;
    int64_t reduced_num_blocks = 5;

    auto metadata_manager_ptr = duckdb::make_uniq<MetadataManager>();
    auto& metadata_manager = *metadata_manager_ptr;
    auto cache = duckdb::make_uniq<Cache>(block_size, nullptr, std::move(metadata_manager_ptr));
    cache->Open(storage_file_path);
    cache->SetMaxCacheSize(initial_num_blocks * block_size);

    duckdb::vector<uint8_t> block_data(block_size, 'x');
    duckdb::string file_path = "/test/set_cache_size_block";

    // Fill cache up to initial capacity
    for (int64_t i = 0; i < initial_num_blocks; ++i) {
        cache->StoreBlock(file_path, i, block_data);
    }

    // Verify all blocks are in cache
    duckdb::vector<uint8_t> dummy_data(block_size, 0);
    for (int64_t i = 0; i < initial_num_blocks; ++i) {
        REQUIRE(cache->RetrieveBlock(file_path, i, dummy_data));
    }

    // Reduce max cache size to hold only 5 blocks, triggering eviction
    cache->SetMaxCacheSize(block_size * reduced_num_blocks);

    // Check cache size and contents after reduction
    REQUIRE(metadata_manager.GetLRUState().size() == reduced_num_blocks);

    // Verify only the most recent 5 blocks are retained in cache
    for (int64_t i = 0; i < initial_num_blocks - reduced_num_blocks; ++i) {
        REQUIRE_FALSE(cache->RetrieveBlock(file_path, i, dummy_data));  // Should be evicted
    }
    for (int64_t i = initial_num_blocks - reduced_num_blocks; i < initial_num_blocks; ++i) {
        REQUIRE(cache->RetrieveBlock(file_path, i, dummy_data));  // Should still be present
    }
}

// Class to simulate a crash during block storage
class CrashingBlockManager : public quackstore::BlockManager {
public:
    using BlockManager::BlockManager;

    void StoreBlock(block_id_t block_id, const duckdb::vector<uint8_t>& data) override {
        if (simulate_crash) {
            // Simulating a crash before storing the block data
            throw std::runtime_error("Simulated crash during block storage!");
        }
        // Call base class's method if no crash
        BlockManager::StoreBlock(block_id, data);
    }

    bool simulate_crash = false;
};

duckdb::vector<uint8_t> InitializeRandomData(size_t size) {
    std::random_device rd;                               // Initialize a random device
    std::mt19937 gen(rd());                              // Seed the generator
    std::uniform_int_distribution<unsigned int> dis(0, 255);  // Create a distribution in [0, 255]

    duckdb::vector<uint8_t> data(size, 0);
    for (auto& byte : data) {
        byte = dis(gen);  // Assign random byte
    }
    return data;
}

TEST_CASE("Cache Simulated Crash Test", "[Cache]") {
    duckdb::string storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    uint64_t block_size = Megabytes(1);
    BlockManagerOptions options{block_size};
    auto crashing_block_mgr_ptr = duckdb::make_uniq<CrashingBlockManager>(options);
    auto& block_mgr = *crashing_block_mgr_ptr; // to use in the test

    // Initialize the cache with the crashing block manager
    auto cache = Cache{block_size, std::move(crashing_block_mgr_ptr), duckdb::make_uniq<MetadataManager>()};

    // Start cache operations
    cache.Open(storage_file_path);

    duckdb::vector<uint8_t> block_data1 = InitializeRandomData(block_size);
    duckdb::vector<uint8_t> block_data2 = InitializeRandomData(block_size);

    // Store first block
    // cache.StoreBlock("file1", 0, block_data1);
    REQUIRE_NOTHROW(cache.StoreBlock("file1", 0, block_data1));

    // Store second block, simulate crash
    block_mgr.simulate_crash = true;
    REQUIRE_THROWS(cache.StoreBlock("file2", 0, block_data2));
    // Switch simulate_crash back to false because StoreBlock will be used in Close during metadata and free list write.
    block_mgr.simulate_crash = false;

    // Reinitialize cache for recovery
    cache.Close();
    Cache recovered_cache(block_size,
                          duckdb::make_uniq<CrashingBlockManager>(options),
                          duckdb::make_uniq<MetadataManager>());
    recovered_cache.Open(storage_file_path);

    // Validate that the first block is stored
    duckdb::vector<uint8_t> retrieved_data(block_size);
    REQUIRE(recovered_cache.RetrieveBlock("file1", 0, retrieved_data));
    REQUIRE(retrieved_data == block_data1);

    // Validate that the second block is not present
    REQUIRE_FALSE(recovered_cache.RetrieveBlock("file2", 0, retrieved_data));
}

TEST_CASE("Metadata should be correctly deallocated before serialization", "[Cache]") {
    duckdb::string storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    const auto BLOCK_SIZE = Bytes(128);

    auto block_mgr_ptr = duckdb::make_uniq<BlockManager>(BlockManagerOptions{BLOCK_SIZE});
    auto& block_mgr_ref = *block_mgr_ptr; // to use in the test

    auto cache = Cache{BLOCK_SIZE, std::move(block_mgr_ptr), duckdb::make_uniq<MetadataManager>()};

    // Start cache operations
    cache.Open(storage_file_path);
    cache.SetMaxCacheSize(Megabytes(10)); // Set a larger cache size to ensure we can store multiple blocks
    REQUIRE(block_mgr_ref.GetMaxBlock() == 0);
    REQUIRE(block_mgr_ref.GetFreeList().empty());

    duckdb::vector<uint8_t> block_data(BLOCK_SIZE, 0);
    const size_t NUM_FILES = 10;
    const size_t NUM_BLOCKS = 10;
    uint8_t byte = 0;
    for(int i = 0; i < NUM_FILES; ++i) {
        duckdb::string file_name = "file" + std::to_string(i);
        // Store multiple blocks for different files
        for(int block_id = 0; block_id < NUM_BLOCKS; ++block_id) {
            std::fill(block_data.begin(), block_data.end(), byte++);
            cache.StoreBlock(file_name, block_id, block_data);
        }
    }
    const auto DATA_BLOCKS = block_mgr_ref.GetMaxBlock();
    REQUIRE(DATA_BLOCKS > 0);

    cache.Flush();
    const auto METADATA_BLOCKS = block_mgr_ref.GetMaxBlock() - DATA_BLOCKS;
    REQUIRE(METADATA_BLOCKS > 0);

    // Calling multiple flushes should not increase the number of blocks / leave zombie blocks
    for(int i = 0; i < NUM_FILES; ++i) {
        duckdb::string file_name = "file" + std::to_string(i);
        cache.StoreFileSize(file_name, 1024); // Store some metadata to mark cache dirty, exact size doesn't really matter here
        cache.Flush();
        REQUIRE(block_mgr_ref.GetMaxBlock() == DATA_BLOCKS + METADATA_BLOCKS);
        REQUIRE(block_mgr_ref.GetFreeList().empty());
    }
}
