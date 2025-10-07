#include <limits>
#include <ctime>

#include "metadata_manager.hpp"

namespace quackstore {

// =============================================================================
// FileMetadata
// =============================================================================

void MetadataManager::FileMetadata::Write(duckdb::WriteStream &ser) const {
    ser.Write(file_size);
    ser.Write<uint32_t>(blocks.size()); // using uint32_t for number of blocks
    for (const auto &block_entry : blocks) {
        const auto &block = block_entry.second;
        ser.Write(block.block_index);
        ser.Write(block.block_id);
        ser.Write(block.checksum);
    }
    ser.Write(__last_modified_deprecated); // Write the last modified timestamp (deprecated field: __last_modified_deprecated)
    ser.Write(last_modified.value); // Write the last modified timestamp
}

MetadataManager::FileMetadata MetadataManager::FileMetadata::Read(duckdb::ReadStream &source, uint32_t version) {
    FileMetadata result;
    switch (version)
    {
        case 1:
            ReadV1(source, result);
        break;
        case 2:
            ReadV2(source, result);
        break;
        case 3:
            ReadV3(source, result);
        break;
        default:
            throw duckdb::IOException("Unsupported file metadata version [" + std::to_string(version) + "]");
        break;
    }
    return result;
}

duckdb::string MetadataManager::FileMetadata::ToString() const {
    duckdb::string result = "{";
    result += " file_size=" + std::to_string(file_size);
    result += " blocks={";
    for (const auto& pair: blocks) {
        const auto& block = pair.second;
        result += " {" + std::to_string(block.block_index) + ": " + std::to_string(block.block_id) + "}";
    }
    result += "}";
    result += " __last_modified_deprecated=" + std::to_string(__last_modified_deprecated);
    result += " last_modified=" + std::to_string(last_modified.value) + " (" + duckdb::Timestamp::ToString(last_modified) + ")";
    result += "}";
    return result;
}

void MetadataManager::FileMetadata::ReadV1(duckdb::ReadStream &source, MetadataManager::FileMetadata& out)
{
    out.file_size = source.Read<uint64_t>();
    uint32_t num_blocks = source.Read<uint32_t>();
    for (uint32_t i = 0; i < num_blocks; ++i) {
        int64_t block_index = source.Read<int64_t>();
        block_id_t block_id = source.Read<int64_t>();
        uint64_t checksum = source.Read<uint64_t>();
        FileMetadataBlockInfo block_info{block_index, block_id, checksum};
        out.blocks[block_id] = block_info;
    }
}
void MetadataManager::FileMetadata::ReadV2(duckdb::ReadStream &source, MetadataManager::FileMetadata& out)
{
    ReadV1(source, out);
    out.__last_modified_deprecated = source.Read<time_t>(); // Legacy field, read but not used
    if (out.__last_modified_deprecated)
    {
        out.last_modified = duckdb::Timestamp::FromTimeT(out.__last_modified_deprecated);
    }
}
void MetadataManager::FileMetadata::ReadV3(duckdb::ReadStream &source, MetadataManager::FileMetadata& out)
{
    ReadV2(source, out);
    out.last_modified = duckdb::timestamp_t{source.Read<int64_t>()};
}

// =============================================================================
// MetadataManager
// =============================================================================

MetadataManager::MetadataManager() : max_cache_size(std::numeric_limits<int64_t>::max()) {}
MetadataManager::~MetadataManager() {}

void MetadataManager::Clear() {
    block_mapping.clear();
    reverse_block_mapping.clear();
    files_metadata.clear();
    lru_list.clear();
    lru_map.clear();
}

block_id_t MetadataManager::GetBlockId(const duckdb::string &file_path, int64_t block_index) const {
    auto it = block_mapping.find({file_path, block_index});
    if (it != block_mapping.end()) {
        return it->second;
    }

    return BlockManager::INVALID_BLOCK_ID;
}

void MetadataManager::RegisterBlock(const duckdb::string &file_path, int64_t block_index, block_id_t block_id,
                                    uint64_t checksum) {
    BlockKey key{file_path, block_index};

    reverse_block_mapping[block_id] = key;
    block_mapping[key] = block_id;

    FileMetadata &file_metadata = files_metadata[file_path];
    FileMetadataBlockInfo block_info{block_index, block_id, checksum};
    file_metadata.blocks[block_id] = block_info;
}

void MetadataManager::UnregisterBlock(block_id_t block_id) {
    auto block_key_it = reverse_block_mapping.find(block_id);
    if (block_key_it != reverse_block_mapping.end()) {
        const BlockKey &key = block_key_it->second;

        // Remove the block from the metadata
        auto file_metadata_it = files_metadata.find(key.file_path);
        if (file_metadata_it != files_metadata.end()) {
            auto &blocks = file_metadata_it->second.blocks;
            blocks.erase(block_id);
            if (blocks.empty()) {
                files_metadata.erase(file_metadata_it);
            }
        }

        // Remove from block_mapping and reverse_block_mapping
        block_mapping.erase(key);
        reverse_block_mapping.erase(block_key_it);
    }

    // Remove from the LRU tracking
    auto lru_map_it = lru_map.find(block_id);
    if (lru_map_it != lru_map.end())
    {
        auto lru_list_it = lru_map_it->second;
        lru_map.erase(block_id);
        lru_list.erase(lru_list_it);
    }
}

void MetadataManager::SetFileSize(const duckdb::string &file_path, int64_t file_size) {
    auto& entry = files_metadata[file_path];
    entry.file_size = file_size;
}

void MetadataManager::SetFileLastModified(const duckdb::string &file_path, duckdb::timestamp_t timestamp) {
    auto& entry = files_metadata[file_path];
    entry.last_modified = timestamp;
}

bool MetadataManager::GetFileMetadata(const duckdb::string &file_path, FileMetadata &file_metadata_out) const {
    auto it = files_metadata.find(file_path);
    if (it == files_metadata.end()) {
        return false;
    }

    file_metadata_out = it->second;
    return true;
}

void MetadataManager::UpdateLRUOrder(block_id_t block_id) {
    if (lru_map.find(block_id) != lru_map.end()) {
        lru_list.erase(lru_map[block_id]);
    }
    lru_list.push_front(block_id);
    lru_map[block_id] = lru_list.begin();
}

void MetadataManager::EvictLRUBlockIfNeeded(std::function<void(block_id_t)> remove_from_storage_func) {
    while (lru_map.size() > max_cache_size) {
        if (!lru_list.empty()) {
            const auto &block_id = lru_list.back();
            // Remove from the storage
            remove_from_storage_func(block_id);
            // Remove from the metadata
            UnregisterBlock(block_id);
        }
    }
}

void MetadataManager::WriteMetadata(MetadataWriter &writer) {
    // Write the number of files' metadata
    writer.Write<uint64_t>(files_metadata.size());

    // Serialize each file's metadata
    for (auto &file_entry : files_metadata) {
        // Serialize the file path
        const duckdb::string &file_path = file_entry.first;
        uint32_t path_size = static_cast<uint32_t>(file_path.size());
        writer.Write<uint32_t>(path_size);
        writer.WriteData(reinterpret_cast<const uint8_t *>(file_path.data()), path_size);

        // Serialize the file metadata
        file_entry.second.Write(writer);
    }

    // Serialize the LRU list
    writer.Write<uint64_t>(lru_list.size());
    for (const auto &block_id : lru_list) {
        writer.Write<int64_t>(block_id);
    }
}

void MetadataManager::ReadMetadata(MetadataReader &reader, uint32_t version) {
    files_metadata.clear();
    block_mapping.clear();
    reverse_block_mapping.clear();

    uint64_t num_files = reader.Read<uint64_t>();
    // Deserialize each file's metadata
    for (uint64_t i = 0; i < num_files; ++i) {
        // Deserialize the file path
        uint32_t path_size = reader.Read<uint32_t>();
        duckdb::string file_path(path_size, '\0');
        reader.ReadData(reinterpret_cast<uint8_t *>(&file_path[0]), path_size);

        // Deserialize the file metadata
        FileMetadata file_metadata = FileMetadata::Read(reader, version);
        files_metadata[file_path] = file_metadata;

        // Update the block mapping with block indices and ids
        for (const auto &block_entry : file_metadata.blocks) {
            const auto &block = block_entry.second;
            BlockKey block_key{file_path, block.block_index};
            block_mapping[block_key] = block.block_id;
            reverse_block_mapping[block.block_id] = block_key;
        }
    }

    // Deserialize and reconstruct the LRU list and map
    uint64_t lru_size = reader.Read<uint64_t>();
    lru_list.clear();
    lru_map.clear();

    for (uint64_t i = 0; i < lru_size; ++i) {
        block_id_t block_id = reader.Read<int64_t>();
        lru_list.push_back(block_id);
        lru_map[block_id] = std::prev(lru_list.end());
    }
}

void MetadataManager::SetMaxCacheSize(idx_t max_cache_size_in_blocks) { max_cache_size = max_cache_size_in_blocks; }

MetadataManager::FileMetadataBlockInfo MetadataManager::GetBlockInfo(const duckdb::string &file_path,
                                                                     block_id_t block_id) const {
    auto file_it = files_metadata.find(file_path);
    if (file_it != files_metadata.end()) {
        const auto &blocks = file_it->second.blocks;
        auto it = blocks.find(block_id);
        if (it != blocks.end()) {
            return it->second;
        }
    }
    throw std::runtime_error("Block info not found for the given file path and block index!");
}

duckdb::vector<MetadataManager::BlockKey> MetadataManager::GetLRUState() const {
    duckdb::vector<BlockKey> lru_state;
    lru_state.reserve(lru_list.size());

    for (const auto &block_id : lru_list) {
        auto it = reverse_block_mapping.find(block_id);
        if (it != reverse_block_mapping.end()) {
            lru_state.push_back(it->second);
        }
    }

    return lru_state;
}

}  // namespace quackstore
