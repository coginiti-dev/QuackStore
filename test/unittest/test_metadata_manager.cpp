#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include "metadata_manager.hpp"

using namespace quackstore;

MetadataManager::FileMetadata GetSampleMetadataV1()
{
    auto result = MetadataManager::FileMetadata{};
    result.file_size = 123456789;
    result.blocks = {
        {1, {11, 1, 111111}},
        {2, {22, 2, 222222}},
        {3, {33, 3, 333333}}
    };
    for(const auto& block : result.blocks) 
    {
        D_ASSERT(block.first == block.second.block_id);
    }
    return result;
}

MetadataManager::FileMetadata GetSampleMetadataV2()
{
    auto result = GetSampleMetadataV1();
    result.__last_modified_deprecated = 1622547800; // some arbitrary timestamp
    return result;
}

TEST_CASE("FileMetadata is constructed properly initialized", "[MetadataManager][FileMetadata]") {
    MetadataManager::FileMetadata metadata;
    CHECK(metadata.file_size == 0);
    CHECK(metadata.blocks.empty());
    CHECK(metadata.__last_modified_deprecated == 0);
    CHECK(metadata.last_modified == duckdb::timestamp_t::epoch());
}

TEST_CASE("FileMetadata gets serialized and deserialized properly v1", "[MetadataManager][FileMetadata]") {
    auto serialized = GetSampleMetadataV1();
    INFO("Serialized metadata: " + serialized.ToString());

    duckdb::MemoryStream mem;
    serialized.Write(mem);

    mem.Rewind();
    auto deserialized = MetadataManager::FileMetadata::Read(mem, 1);
    INFO("Deserialized metadata: " + deserialized.ToString());

    CHECK(deserialized.file_size == serialized.file_size);
    CHECK(deserialized.blocks.size() == serialized.blocks.size());
    for (const auto& [in_id, in_block] : serialized.blocks) {
        REQUIRE(deserialized.blocks.find(in_id) != deserialized.blocks.end());
        const auto& out_block = deserialized.blocks.at(in_id);
        CHECK(out_block.block_index == in_block.block_index);
        CHECK(out_block.block_id == in_block.block_id);
        CHECK(out_block.checksum == in_block.checksum);
    }
    REQUIRE(serialized.__last_modified_deprecated == 0);
    CHECK(deserialized.__last_modified_deprecated == 0);
    REQUIRE(serialized.last_modified == duckdb::timestamp_t::epoch());
    CHECK(deserialized.last_modified == duckdb::timestamp_t::epoch());
}

TEST_CASE("FileMetadata gets serialized and deserialized properly v2", "[MetadataManager][FileMetadata]") {
    const auto serialized = GetSampleMetadataV2();
    INFO("Serialized metadata: " + serialized.ToString());

    duckdb::MemoryStream mem;
    serialized.Write(mem);

    mem.Rewind();
    auto deserialized = MetadataManager::FileMetadata::Read(mem, 2);
    INFO("Deserialized metadata: " + deserialized.ToString());

    CHECK(deserialized.file_size == serialized.file_size);
    CHECK(deserialized.blocks.size() == serialized.blocks.size());
    for (const auto& [in_id, in_block] : serialized.blocks) {
        REQUIRE(deserialized.blocks.find(in_id) != deserialized.blocks.end());
        const auto& out_block = deserialized.blocks.at(in_id);
        CHECK(out_block.block_index == in_block.block_index);
        CHECK(out_block.block_id == in_block.block_id);
        CHECK(out_block.checksum == in_block.checksum);
    }
    REQUIRE(serialized.__last_modified_deprecated > 0);
    CHECK(deserialized.__last_modified_deprecated > 0);
    REQUIRE(serialized.last_modified == duckdb::timestamp_t::epoch());
    CHECK(deserialized.last_modified == duckdb::timestamp_t::epoch());
}

TEST_CASE("FileMetadata gets serialized and deserialized properly v3 (read v2)", "[MetadataManager][FileMetadata]") {
    const auto serialized = GetSampleMetadataV2();
    INFO("Serialized metadata: " + serialized.ToString());

    duckdb::MemoryStream mem;
    serialized.Write(mem);

    mem.Rewind();
    auto deserialized = MetadataManager::FileMetadata::Read(mem, 3);
    INFO("Deserialized metadata: " + deserialized.ToString());

    CHECK(deserialized.file_size == serialized.file_size);
    CHECK(deserialized.blocks.size() == serialized.blocks.size());
    for (const auto& [in_id, in_block] : serialized.blocks) {
        REQUIRE(deserialized.blocks.find(in_id) != deserialized.blocks.end());
        const auto& out_block = deserialized.blocks.at(in_id);
        CHECK(out_block.block_index == in_block.block_index);
        CHECK(out_block.block_id == in_block.block_id);
        CHECK(out_block.checksum == in_block.checksum);
    }
    REQUIRE(serialized.__last_modified_deprecated > 0);
    CHECK(deserialized.__last_modified_deprecated > 0);
    REQUIRE(serialized.last_modified > duckdb::timestamp_t::epoch());
    CHECK(deserialized.last_modified > duckdb::timestamp_t::epoch());
}
