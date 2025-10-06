#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/common/file_opener.hpp>

#include "block_manager.hpp"

using namespace quackstore;

TEST_CASE("BlockCacheDataFileHeader size", "[BlockManager]")
{
    CHECK(BlockCacheDataFileHeader::Size() == 44);
}

TEST_CASE("Make sure block manager uses the correct path (CreateNewDatabase)", "[BlockManager]") 
{
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }
    REQUIRE_FALSE(local_fs->FileExists(storage_file_path));

    BlockManager block_mgr({Kilobytes(1)});

    SECTION("CreateNewDatabase") {
        SECTION("When cache does not exist") {
            REQUIRE_FALSE(local_fs->FileExists(storage_file_path));
            block_mgr.CreateNewDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
        }
        SECTION("When cache already exists") {
            block_mgr.CreateNewDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
            block_mgr.CreateNewDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
        }
    }
    SECTION("LoadExistingDatabase") {
        SECTION("When cache does not exist") {
            REQUIRE_FALSE(local_fs->FileExists(storage_file_path));
            REQUIRE_THROWS_AS(block_mgr.LoadExistingDatabase(storage_file_path), duckdb::IOException);
        }
        SECTION("When cache already exists") {
            block_mgr.CreateNewDatabase(storage_file_path);
            REQUIRE(local_fs->FileExists(storage_file_path));
            block_mgr.LoadExistingDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
        }
    }
    SECTION("LoadOrCreateDatabase") {
        SECTION("Create new") {
            REQUIRE_FALSE(local_fs->FileExists(storage_file_path));
            block_mgr.LoadOrCreateDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
        }
        SECTION("Load existing") {
            block_mgr.CreateNewDatabase(storage_file_path);
            REQUIRE(local_fs->FileExists(storage_file_path));

            block_mgr.LoadOrCreateDatabase(storage_file_path);
            CHECK(local_fs->FileExists(storage_file_path));
        }
    }
}

TEST_CASE("Double deallocation of a block is no-op", "[BlockManager]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();

    // Prepare few blocks
    const int TOTAL_BLOCKS = 10;
    auto block_mgr = BlockManager{{Kilobytes(1)}};
    block_mgr.CreateNewDatabase(storage_file_path);
    for(int i = 0; i < TOTAL_BLOCKS; ++i) {
        block_mgr.AllocBlock();
        CHECK(block_mgr.GetMaxBlock() == i + 1);
    }
    REQUIRE(block_mgr.GetMaxBlock() == TOTAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().empty());

    // Deallocate blocks one by one and check that the free list is updated correctly.
    // Also check that further attempts to deallocate the same block do not change the free list.
    for(int i = 0; i < TOTAL_BLOCKS; ++i) {
        INFO("Deallocating block " << i);
        block_mgr.MarkBlockAsFree(i);
        const auto free_list_after_one_dealloc = block_mgr.GetFreeList();
        CHECK(free_list_after_one_dealloc.size() == i + 1);
        CHECK(free_list_after_one_dealloc.find(i) != free_list_after_one_dealloc.end());

        // further attempts to dealloc do nothing
        for(int retry = 1; retry <= 3; ++retry)
        {
            INFO("Extra retry #" << retry << " to deallocate block " << i);
            block_mgr.MarkBlockAsFree(i);
            CHECK(block_mgr.GetFreeList() == free_list_after_one_dealloc);
        }
   }

    REQUIRE(block_mgr.GetMaxBlock() == TOTAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().size() == TOTAL_BLOCKS);
}

TEST_CASE("FreeList saving/loading works correctly", "[BlockManager]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();

    BlockManagerOptions options;
    options.block_size = Kilobytes(1);

    // Write free list to storage
    std::set<block_id_t> original_free_list;
    {
        auto block_mgr = duckdb::make_uniq<quackstore::BlockManager>(options);
        block_mgr->CreateNewDatabase(storage_file_path);

        // Allocate blocks
        for (int i = 0; i < 32; ++i) {
            block_id_t block_id = block_mgr->AllocBlock();
            if (i % 3 == 0) {
                original_free_list.insert(block_id);
            }
        }

        // Mark all those blocks as free
        for (const auto &block_id : original_free_list) {
            block_mgr->MarkBlockAsFree(block_id);
        }

        // Save the free list to storage
        block_mgr->Flush();

        // The first freed block was reused for the free list save operation
        // so we remove it from the expected result
        original_free_list.erase(original_free_list.begin());
    }

    // Read free list from storage and verify
    {
        auto block_mgr = duckdb::make_uniq<quackstore::BlockManager>(options);
        block_mgr->LoadExistingDatabase(storage_file_path);

        // Verify that the free list matches the original
        REQUIRE(block_mgr->GetFreeList() == original_free_list);
    }
}


TEST_CASE("Attempts to dealloc blocks outside the allocated range should fail", "[BlockManager]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();

    // Prepare few blocks
    const int TOTAL_BLOCKS = 5;
    auto block_mgr = BlockManager{{Kilobytes(1)}};
    block_mgr.CreateNewDatabase(storage_file_path);
    for(int i = 0; i < TOTAL_BLOCKS; ++i) {
        block_mgr.AllocBlock();
    }
    REQUIRE(block_mgr.GetMaxBlock() == TOTAL_BLOCKS);

    REQUIRE_THROWS_AS(
        block_mgr.MarkBlockAsFree(BlockManager::INVALID_BLOCK_ID), 
        duckdb::InvalidInputException);

    for(int num_beyond_max_block = 0; num_beyond_max_block < 5; ++num_beyond_max_block)
    {
        REQUIRE_THROWS_AS(
            block_mgr.MarkBlockAsFree(TOTAL_BLOCKS + num_beyond_max_block), 
            duckdb::InvalidInputException);
    }
    for(int num_behind_zero = 5; num_behind_zero > 0; --num_behind_zero)
    {
        REQUIRE_THROWS_AS(
            block_mgr.MarkBlockAsFree(0 - num_behind_zero), 
            duckdb::InvalidInputException);
    }
    for(int i = 0; i < TOTAL_BLOCKS; ++i)
    {
        REQUIRE_NOTHROW(block_mgr.MarkBlockAsFree(i));
    }
}

TEST_CASE("FreeList should be correctly deallocated before serialization", "[BlockManager]") {
    auto storage_file_path = "/tmp/cache.bin";
    auto local_fs = duckdb::FileSystem::CreateLocal();
    if (local_fs->FileExists(storage_file_path)) {
        local_fs->RemoveFile(storage_file_path);
    }

    const auto BLOCK_SIZE = Bytes(64);
    const auto CHAINED_BLOCK_PAYLOAD_SIZE = BLOCK_SIZE - sizeof(block_id_t); //exluding id of a next chained block

    // Number of bytes required to store the free list
    auto get_list_data_size = [](size_t size) {
        return sizeof(uint64_t) + size * sizeof(block_id_t);
    };
    // Number of blocks required to store the free list
    auto num_blocks_required = [=](size_t data_size){
        return static_cast<size_t>(std::ceil(static_cast<double>(data_size) / CHAINED_BLOCK_PAYLOAD_SIZE));
    };

    auto block_mgr = BlockManager{BlockManagerOptions{BLOCK_SIZE}};
    /////////////////////////////////////////////
    // STEP 1: Prep
    /////////////////////////////////////////////
    block_mgr.CreateNewDatabase(storage_file_path);
    // Logical layout:
    // * Free block ids (free list):  []
    // Cache layout:
    // * Serialized Free List Blocks: [] 

    duckdb::set<block_id_t> manually_allocated_blocks;
    // Allocate initial blocks
    const auto NUM_INITIAL_BLOCKS = 100;
    for (int i = 0; i < NUM_INITIAL_BLOCKS; ++i) {
        auto id = block_mgr.AllocBlock();
        CHECK(block_mgr.GetMaxBlock() == i + 1);
        CHECK(block_mgr.GetFreeList().empty());

        manually_allocated_blocks.insert(id);
    }
    // Logical layout:
    // * Free block ids (free list):  [] 
    // Cache layout:
    // * Serialized Free List Blocks: [] 
    REQUIRE(manually_allocated_blocks.size() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().empty());

    block_mgr.Flush();
    // Logical layout:
    // * Free block ids (free list):  [] 
    // Cache layout:
    // * Serialized Free List Blocks: [] 
    REQUIRE(manually_allocated_blocks.size() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().empty());

    /////////////////////////////////////////////
    // STEP 2: Release some blocks back to the free list
    /////////////////////////////////////////////
    const auto NUM_INITIALLY_RELEASED_BLOCKS = NUM_INITIAL_BLOCKS - 1;
    REQUIRE(NUM_INITIALLY_RELEASED_BLOCKS < NUM_INITIAL_BLOCKS);
    for(int i = 0; i < NUM_INITIALLY_RELEASED_BLOCKS; ++i) {
        auto id = *manually_allocated_blocks.begin();
        block_mgr.MarkBlockAsFree(id);
        CHECK(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
        CHECK(block_mgr.GetFreeList().size() == i + 1);

        manually_allocated_blocks.erase(id);
    }
    // Logical layout:
    // * Free block ids (free list):  [ 0 .. 98] (*)
    // Cache layout:
    // * Serialized Free List Blocks: [] 
    REQUIRE(manually_allocated_blocks.empty() == false);
    CHECK(manually_allocated_blocks.size() == NUM_INITIAL_BLOCKS - NUM_INITIALLY_RELEASED_BLOCKS);
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    CHECK(block_mgr.GetFreeList().size() == NUM_INITIALLY_RELEASED_BLOCKS);
    CHECK(*block_mgr.GetFreeList().begin() == 0);
    CHECK(*block_mgr.GetFreeList().rbegin() == NUM_INITIALLY_RELEASED_BLOCKS - 1);

    // We'll have to allocate a number of blocks to save the free list.
    const auto NUM_BLOCKS_FOR_INITIAL_FREE_LIST = num_blocks_required( get_list_data_size(block_mgr.GetFreeList().size()) );
    block_mgr.Flush();
    // Logical layout:
    // * Free block ids (free list):  [15 .. 99]
    // Cache layout:
    // * Serialized Free List Blocks: [ 0 .. 14]
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    CHECK(block_mgr.GetFreeList().size() == NUM_INITIALLY_RELEASED_BLOCKS - NUM_BLOCKS_FOR_INITIAL_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().begin() == NUM_BLOCKS_FOR_INITIAL_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().rbegin() == NUM_INITIALLY_RELEASED_BLOCKS - 1);

    /////////////////////////////////////////////
    // STEP 2: Allocate more blocks - exhaust the free list
    /////////////////////////////////////////////
    const auto NUM_ADDITIONAL_ALLOCATED_BLOCKS = block_mgr.GetFreeList().size();
    for (int i = 0; i < NUM_ADDITIONAL_ALLOCATED_BLOCKS; ++i) {
        auto id = block_mgr.AllocBlock();
        REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
        manually_allocated_blocks.insert(id);
    }
    // Logical layout:
    // * Free block ids (free list):  [] (*)
    // Cache layout:
    // * Serialized Free List Blocks: [ 0 .. 14]
    REQUIRE(manually_allocated_blocks.empty() == false);
    CHECK(manually_allocated_blocks.size() == NUM_INITIAL_BLOCKS - NUM_INITIALLY_RELEASED_BLOCKS + NUM_ADDITIONAL_ALLOCATED_BLOCKS);
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    CHECK(block_mgr.GetFreeList().empty());

    // After releasing blocks [ 0 .. 14] the free list will be populated with 15 block ids
    const auto NUM_BLOCKS_FOR_NEXT_FREE_LIST = num_blocks_required(get_list_data_size(NUM_BLOCKS_FOR_INITIAL_FREE_LIST));
    block_mgr.Flush();
    // Logical layout:
    // * Free block ids (free list):  [ 3 .. 14] (*)
    // Cache layout:
    // * Serialized Free List Blocks: [ 0 ..  2] (*)
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().size() == NUM_BLOCKS_FOR_INITIAL_FREE_LIST - NUM_BLOCKS_FOR_NEXT_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().begin() == NUM_BLOCKS_FOR_NEXT_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().rbegin() == NUM_BLOCKS_FOR_INITIAL_FREE_LIST - 1);


    /////////////////////////////////////////////
    // STEP 3: Release all manually allocated blocks
    /////////////////////////////////////////////
    while(!manually_allocated_blocks.empty()) {
        auto it = manually_allocated_blocks.begin();
        block_mgr.MarkBlockAsFree(*it);
        manually_allocated_blocks.erase(it);
    }
    // Logical layout:
    // * Free block ids (free list):  [ 3 .. 99] (*)
    // Cache layout:
    // * Serialized Free List Blocks: [ 0 ..  2]
    REQUIRE(manually_allocated_blocks.empty());
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().empty() == false);
    CHECK(*block_mgr.GetFreeList().begin() == NUM_BLOCKS_FOR_NEXT_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().rbegin() == NUM_INITIAL_BLOCKS - 1);

    const auto NUM_BLOCKS_FOR_FINAL_FREE_LIST = num_blocks_required(get_list_data_size(NUM_BLOCKS_FOR_NEXT_FREE_LIST + 
        block_mgr.GetFreeList().size()));
    block_mgr.Flush();
    // Logical layout:
    // * Free block ids (free list):  [15 .. 99] (*)
    // Cache layout:
    // * Serialized Free List Blocks: [ 0 .. 14] (*)
    REQUIRE(block_mgr.GetMaxBlock() == NUM_INITIAL_BLOCKS);
    REQUIRE(block_mgr.GetFreeList().empty() == false);
    CHECK(block_mgr.GetFreeList().size() == NUM_INITIAL_BLOCKS - NUM_BLOCKS_FOR_FINAL_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().begin() == NUM_BLOCKS_FOR_FINAL_FREE_LIST);
    CHECK(*block_mgr.GetFreeList().rbegin() == NUM_INITIAL_BLOCKS - 1);
}