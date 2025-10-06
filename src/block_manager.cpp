#include <duckdb.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/read_stream.hpp>

#include "block_manager.hpp"
#include "metadata_reader.hpp"
#include "metadata_writer.hpp"

namespace 
{
    const uint32_t BLOCK_CACHE_DATA_FILE_VERSION_NUMBER = 2;
}

namespace cachefs {

// =============================================================================
// BlockCacheDataFileHeader
// =============================================================================

constexpr static uint32_t MAGIC_BYTE_SIZE = 8;
constexpr static duckdb::data_t MAGIC_BYTES[MAGIC_BYTE_SIZE] = { 'C', 'O', 'G', 'B', 'S', 'T', 'O', 'R' };

void BlockCacheDataFileHeader::Write(duckdb::WriteStream &ser) {
    D_ASSERT(MAGIC_BYTE_SIZE == sizeof(MAGIC_BYTES) / sizeof(MAGIC_BYTES[0]));
    ser.WriteData(duckdb::const_data_ptr_cast(MAGIC_BYTES), MAGIC_BYTE_SIZE);
    ser.Write(version);
    ser.Write(meta_block);
    ser.Write(free_list);
    ser.Write(block_count);
    ser.Write(block_size);
}

BlockCacheDataFileHeader BlockCacheDataFileHeader::Read(duckdb::ReadStream &source) {
    duckdb::data_t magic_bytes[MAGIC_BYTE_SIZE];
    BlockCacheDataFileHeader header;
    source.ReadData(magic_bytes, MAGIC_BYTE_SIZE);
    if (memcmp(magic_bytes, MAGIC_BYTES, MAGIC_BYTE_SIZE) != 0) {
        throw duckdb::IOException("The file is not a valid block cache file!");
    }
    header.version = source.Read<uint32_t>();
    header.meta_block = source.Read<int64_t>();
    header.free_list = source.Read<int64_t>();
    header.block_count = source.Read<uint64_t>();
    header.block_size = source.Read<uint64_t>();

    return header;
}

size_t BlockCacheDataFileHeader::Size() {
    size_t size = 0;
    size += MAGIC_BYTE_SIZE; // magic bytes
    size += sizeof(decltype(version));
    size += sizeof(decltype(meta_block));
    size += sizeof(decltype(free_list));
    size += sizeof(decltype(block_count));
    size += sizeof(decltype(block_size));
    return size;
}

// =============================================================================
// BlockManager
// =============================================================================

BlockManager::BlockManager(const BlockManagerOptions &options)
    : fs(duckdb::FileSystem::CreateLocal()), options(options) {
    if (options.block_size < Bytes(16)) {
        throw duckdb::IOException("The block size can't be smaller than 16 bytes");
    }
}

BlockManager::~BlockManager() { Close(); }

void BlockManager::Close() {
    if (IsOpen()) {
        Flush();
    }
    CloseInternal();
}

void BlockManager::Clear() {
    const auto was_open = IsOpen();
    // Save the path before closing the handle
    const auto path = was_open ? handle->GetPath() : duckdb::string{};

    CloseInternal(); // Not using Close() here to avoid flushing the file
    if (was_open && fs->FileExists(path)) {
        fs->RemoveFile(path);
    }
}

BlockCacheDataFileHeader BlockManager::LoadOrCreateDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out) {
    if (fs->FileExists(path)) {
        return LoadExistingDatabase(path, out);
    } else {
        return CreateNewDatabase(path, out);
    }
}

BlockCacheDataFileHeader BlockManager::CreateNewDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out) {
    Close();
    D_ASSERT(max_block == 0);
    D_ASSERT(meta_block_id == INVALID_BLOCK_ID);
    D_ASSERT(free_list_id == INVALID_BLOCK_ID);

    handle =
        fs->OpenFile(path, duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW | duckdb::FileFlags::FILE_FLAGS_WRITE | duckdb::FileFlags::FILE_FLAGS_READ);
    if (!handle) {
        throw duckdb::IOException("Failed to open block data cache file: \"%s\"!", path);
    }

    BlockCacheDataFileHeader header;
    header.version = BLOCK_CACHE_DATA_FILE_VERSION_NUMBER;
    header.meta_block = meta_block_id;
    header.free_list = free_list_id;
    header.block_count = max_block;

    duckdb::MemoryStream mem;
    header.Write(mem);

    handle->Write(mem.GetData(), mem.GetPosition(), 0);
    handle->Sync();

    if (out) *out = LoadResult::CREATED_NEW;
    return header;
}

BlockCacheDataFileHeader BlockManager::LoadExistingDatabase(const duckdb::string &path, duckdb::optional_ptr<LoadResult> out) {
    Close();
    D_ASSERT(max_block == 0);
    D_ASSERT(meta_block_id == INVALID_BLOCK_ID);
    D_ASSERT(free_list_id == INVALID_BLOCK_ID);

    handle = fs->OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE | duckdb::FileFlags::FILE_FLAGS_READ);
    if (!handle) {
        throw duckdb::IOException("Failed to open block data cache file: \"%s\"!", path);
    }

    // Read the header from the file
    duckdb::vector<uint8_t> header_data(BlockCacheDataFileHeader::Size());
    handle->Read(header_data.data(), header_data.size(), 0);

    duckdb::MemoryStream mem(header_data.data(), header_data.size());
    auto header = BlockCacheDataFileHeader::Read(mem);

    // Initialize state
    max_block = header.block_count;
    meta_block_id = header.meta_block;
    free_list_id = header.free_list;

    if (header.block_size != options.block_size) {
        throw duckdb::IOException(
            "cannot initialize the same block storage with a different block size: provided block "
            "size: %llu, file block size: %llu",
            options.block_size, header.block_size);
    }

    LoadFreeList();

    if (out) *out = LoadResult::LOADED_EXISTING;
    return header;
}

void BlockManager::Flush()
{
    ValidateHandle();

    SaveFreeList();
    WriteHeader();
}

void BlockManager::WriteHeader() {
    ValidateHandle();

    BlockCacheDataFileHeader header;
    header.version = BLOCK_CACHE_DATA_FILE_VERSION_NUMBER;
    header.meta_block = meta_block_id;
    header.free_list = free_list_id;
    header.block_count = max_block;
    header.block_size = options.block_size;

    duckdb::MemoryStream mem;
    header.Write(mem);

    handle->Write(mem.GetData(), mem.GetPosition(), 0);
    handle->Sync();
}

size_t BlockManager::MarkChainedBlocksAsFree(block_id_t block_id) {
    size_t num_deallocated_blocks = 0;

    MetadataReader reader(*this);
    while (reader.ReadBlock(block_id)) {
        auto next_block_id = reader.GetNextBlockId();
        MarkBlockAsFree(block_id);
        block_id = next_block_id;
        ++num_deallocated_blocks;
    }

    return num_deallocated_blocks;
}

block_id_t BlockManager::AllocBlock() {
    block_id_t block_id;
    if (!free_list.empty()) {
        // The free list is not empty, take first element from it
        block_id = *free_list.begin();
        free_list.erase(free_list.begin());
    } else {
        block_id = max_block++;
    }

    return block_id;
}

void BlockManager::StoreBlock(block_id_t block_id, const duckdb::vector<uint8_t> &data) {
    ValidateBlockId(block_id);
    ValidateHandle();

    auto offset = GetBlockOffset(block_id);
    handle->Write(const_cast<duckdb::data_ptr_t>(data.data()), data.size(), offset);
}

void BlockManager::RetrieveBlock(block_id_t block_id, duckdb::vector<uint8_t> &data) {
    ValidateBlockId(block_id);
    ValidateHandle();

    auto offset = GetBlockOffset(block_id);
    handle->Read(data.data(), options.block_size, offset);
}

void BlockManager::MarkBlockAsFree(block_id_t block_id) {
    ValidateBlockId(block_id);
 
    if (free_list.insert(block_id).second == false) {
        // Block is already freed. Do nothing.
        return;
    }
}

uint64_t BlockManager::GetBlockSize() const { return options.block_size; }

block_id_t BlockManager::GetMetaBlockID() {
    if (meta_block_id != INVALID_BLOCK_ID) {
        return meta_block_id;
    }

    meta_block_id = AllocBlock();
    MetadataWriter writer(*this, meta_block_id); // The writer should prepare the metadata block correctly
    return meta_block_id;
}

const duckdb::set<block_id_t> &BlockManager::GetFreeList() const { return free_list; }
block_id_t BlockManager::GetMaxBlock() const { return max_block; }

// =============================================================================
// Private methods
// =============================================================================

uint64_t BlockManager::GetBlockOffset(block_id_t block_id) { 
    ValidateBlockId(block_id);
    return BLOCK_START + block_id * options.block_size; 
}

void BlockManager::SaveFreeList() {
    MarkChainedBlocksAsFree(free_list_id);
    free_list_id = INVALID_BLOCK_ID;
    if (free_list.empty())
    {
        return;
    }

    free_list_id = AllocBlock();
    MetadataWriter writer(*this, free_list_id);
    uint64_t num_blocks = static_cast<uint64_t>(free_list.size());
    writer.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&num_blocks), sizeof(num_blocks));
    for (const auto &block_id : free_list) {
        writer.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&block_id), sizeof(block_id));
    }
}

void BlockManager::LoadFreeList() {
    if (free_list_id == INVALID_BLOCK_ID) {
        // No free list stored
        return;
    }

    MetadataReader reader(*this, free_list_id);

    uint64_t num_blocks;
    reader.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&num_blocks), sizeof(num_blocks));

    free_list.clear();

    for (uint64_t i = 0; i < num_blocks; ++i) {
        block_id_t free_block;
        reader.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&free_block), sizeof(free_block));
        free_list.insert(free_block);
    }
}

void BlockManager::ValidateBlockId(block_id_t block_id) const {
    if (block_id == INVALID_BLOCK_ID) {
        throw duckdb::InvalidInputException("Block ID cannot be INVALID_BLOCK_ID");
    }
    if (block_id < 0) {
        throw duckdb::InvalidInputException("Block ID cannot be negative", {{"block_id", std::to_string(block_id)}});
    }
    if (block_id >= max_block) {
        throw duckdb::InvalidInputException("Block ID cannot exceed max_block", {{"block_id", std::to_string(block_id)}, {"max_block", std::to_string(max_block)}});
    }
}

void BlockManager::ValidateHandle() const {
    if (!IsOpen()) {
        throw duckdb::IOException("BlockManager is not open. Cannot perform operation.");
    }
}

void BlockManager::CloseHandle() {
    if (IsOpen()) {
        handle->Close();
        handle = nullptr;
    }
}

void BlockManager::CloseInternal()
{
    max_block = 0;
    meta_block_id = INVALID_BLOCK_ID;
    free_list_id = INVALID_BLOCK_ID;
    free_list.clear();

    CloseHandle();
}

}  // namespace cachefs
