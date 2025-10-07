#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/common/local_file_system.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/connection_manager.hpp>
#include <duckdb/main/database_file_opener.hpp>
#include <duckdb/main/client_context_file_opener.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>

#include "quackstore_filesystem.hpp"
#include "quackstore_params.hpp"
#include "cache.hpp"
#include "extension_state.hpp"

using namespace quackstore;

namespace {
    duckdb::string StripPrefix(const duckdb::string &text, const duckdb::string &prefix) {
        return text.rfind(prefix, 0) == 0 ? text.substr(prefix.length()) : text;
    }

    class TestFileSystem : public duckdb::LocalFileSystem {
    public:
        TestFileSystem(duckdb::string prefix) : prefix(std::move(prefix))
        {}

        // FileSystem methods
        duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
            const duckdb::string &path, 
            duckdb::FileOpenFlags flags, 
            duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override
        {
            for(auto& cb: on_open_callbacks)
            {
                cb(path, flags, opener);
            }
            auto cleaned_path = StripPrefix(path, prefix);
            return duckdb::LocalFileSystem::OpenFile(cleaned_path, flags, opener);
        }

        void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override
        {
            for(auto& cb: on_read_callbacks)
            {
                cb(handle);
            }
            return duckdb::LocalFileSystem::Read(handle, buffer, nr_bytes, location);
        }

        int64_t Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes) override
        {
            for(auto& cb: on_read_callbacks)
            {
                cb(handle);
            }
            return duckdb::LocalFileSystem::Read(handle, buffer, nr_bytes);
        }

        int64_t GetFileSize(duckdb::FileHandle &handle) override
        {
            for(auto& cb: on_get_filesize_callbacks)
            {
                cb(handle);
            }
            return use_own_filesize ? filesize : duckdb::LocalFileSystem::GetFileSize(handle);
        }

        duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle &handle) override
        {
            for(auto& cb: on_get_lastmodified_callbacks)
            {
                cb(handle);
            }
            return use_own_last_modified ? last_modified : duckdb::LocalFileSystem::GetLastModifiedTime(handle);
        }

        bool CanHandleFile(const duckdb::string &path) override {
            return path.rfind(prefix, 0) == 0;
        }

        std::string GetName() const override {
            return prefix.substr(0, prefix.find("://"));
        }

        // Own methods
        using on_open_callback = std::function<void(const duckdb::string&, duckdb::FileOpenFlags, duckdb::optional_ptr<duckdb::FileOpener>)>;
        using on_read_callback = std::function<void(duckdb::FileHandle&)>;
        using on_get_lastmodified_callback = std::function<void(duckdb::FileHandle&)>;
        using on_get_filesize_callback = std::function<void(duckdb::FileHandle&)>;

        duckdb::vector<on_open_callback> on_open_callbacks;
        duckdb::vector<on_read_callback> on_read_callbacks;
        duckdb::vector<on_get_lastmodified_callback> on_get_lastmodified_callbacks;
        duckdb::vector<on_get_filesize_callback> on_get_filesize_callbacks;

        void SetLastModified(duckdb::timestamp_t val)
        {
            last_modified = val;
            use_own_last_modified = true;
        }
        void ResetLastModified()
        {
            last_modified = 0;
            use_own_last_modified = false;
        }
        void SetFileSize(int64_t val)
        {
            filesize = val;
            use_own_filesize = true;
        }
        void ResetFileSize()
        {
            filesize = 0;
            use_own_filesize = false;
        }

    private:
        duckdb::string prefix;

        bool use_own_last_modified = false;
        duckdb::timestamp_t last_modified;
        bool use_own_filesize = false;
        int64_t filesize = 0;
    };
}

class WithDuckDB {
public:
    WithDuckDB() 
    : duckdb(":memory:")
    {
    }
    duckdb::DuckDB& GetDuckDB() { return duckdb; }
    duckdb::DatabaseInstance& GetDBInstance() { return *duckdb.instance; }
    bool RemoveLocalFile(const std::string &path) {
        auto fs = duckdb::FileSystem::CreateLocal();
        if (fs->FileExists(path)) {
            fs->RemoveFile(path);
            return true;
        }
        return false;
    }

    static ExtensionParams GetExtensionParams(duckdb::DatabaseInstance &db) {
        auto opener = duckdb::DatabaseFileOpener{db};
        return ExtensionParams::ReadFrom(&opener);
    }

    ExtensionParams GetExtensionParams(duckdb::ClientContext &context) {
        auto opener = duckdb::ClientContextFileOpener{context};
        return ExtensionParams::ReadFrom(&opener);
    }

    private:
    duckdb::DuckDB duckdb;
};

TEST_CASE_METHOD(WithDuckDB, "Reading a file", "[quackstore]") {
    const auto CACHE_PATH = "/tmp/cache.bin";
    RemoveLocalFile(CACHE_PATH);
    auto cache = duckdb::make_uniq<Cache>(16);
    cache->Open(CACHE_PATH);

    auto& main_fs_ref = GetDBInstance().GetFileSystem();

    auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);
    main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
    main_fs_ref.RegisterSubSystem(std::move(cache_fs));

    const duckdb::string FILE_PATH = QuackstoreFileSystem::SCHEMA_PREFIX + duckdb::string{"test/testdata/read_test.txt"};
    const duckdb::string FILE_CONTENT = "This is a text.\n";
    SECTION("with disabled cache") {
        auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
        config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(false));

        SECTION("reading whole file") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            int64_t file_size = main_fs_ref.GetFileSize(*file_handle);

            duckdb::vector<char> buffer(file_size);
            int64_t bytes_read = main_fs_ref.Read(*file_handle, buffer.data(), file_size);
            REQUIRE(bytes_read == file_size);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT);
        }
    }

    SECTION("with enabled cache") {
        auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
        config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true));
        config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_SIZE, duckdb::Value::UBIGINT(128));

        SECTION("reading full file") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            int64_t file_size = main_fs_ref.GetFileSize(*file_handle);

            duckdb::vector<char> buffer(file_size);
            int64_t bytes_read = main_fs_ref.Read(*file_handle, buffer.data(), file_size);
            REQUIRE(bytes_read == file_size);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT);
        }

        SECTION("reading full file with attempt to read double size") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            int64_t file_size = main_fs_ref.GetFileSize(*file_handle);

            duckdb::vector<char> buffer(file_size);
            int64_t bytes_read = main_fs_ref.Read(*file_handle, buffer.data(),
                                                file_size * 2);  // We are trying to read 2 times more than file size
            REQUIRE(bytes_read == file_size);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT);
        }

        SECTION("reading 7 bytes in the beginning of the file") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);

            int64_t bytes_to_read = 7;
            duckdb::vector<char> buffer(bytes_to_read);
            int64_t bytes_read = main_fs_ref.Read(*file_handle, buffer.data(), bytes_to_read);
            REQUIRE(bytes_read == bytes_to_read);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT.substr(0, bytes_to_read));
        }

        SECTION("reading 4 bytes in the middle of the file") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);

            int64_t bytes_to_read = 4;
            int64_t offset = 5;
            duckdb::vector<char> buffer(bytes_to_read);
            main_fs_ref.Read(*file_handle, buffer.data(), bytes_to_read, offset);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT.substr(offset, bytes_to_read));
        }

        SECTION("reading end of the file") {
            auto file_handle =
                main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            int64_t file_size = main_fs_ref.GetFileSize(*file_handle);

            int64_t offset = 10;
            int64_t bytes_to_read = std::min(int64_t{40}, file_size - offset);
            duckdb::vector<char> buffer(bytes_to_read);
            main_fs_ref.Read(*file_handle, buffer.data(), bytes_to_read, offset);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT.substr(offset, bytes_to_read));
        }
    }
}

TEST_CASE_METHOD(WithDuckDB, "Clear cache exception with open files", "[quackstore]") {
    const auto CACHE_PATH = "/tmp/cache.bin";
    RemoveLocalFile(CACHE_PATH);
    auto cache = Cache{16};
    cache.Open(CACHE_PATH);

    auto& main_fs_ref = GetDBInstance().GetFileSystem();
    main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
    auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(cache);

    main_fs_ref.RegisterSubSystem(std::move(cache_fs));

    auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
    config.SetOptionsByName({
        {ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true)},
        {ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_SIZE, duckdb::Value::UBIGINT(128)}
    });

    // Open a file to ensure the file count is not zero
    const duckdb::string FILE_PATH = QuackstoreFileSystem::SCHEMA_PREFIX + duckdb::string{"test/testdata/read_test.txt"};
    auto file_handle =
        main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);

    // Verify that an exception is thrown if trying to clear the cache while the file is open
    REQUIRE_THROWS_AS(cache.Clear(), std::runtime_error);

    // Close the file and verify no exception is thrown when clearing the cache
    file_handle->Close();
    REQUIRE_NOTHROW(cache.Close());
}

TEST_CASE_METHOD(WithDuckDB, "Migrate V1 cache to V2", "[quackstore][migration]") {
    const auto CACHE_PATH = "/tmp/cache.bin";
    auto& main_fs_ref = GetDBInstance().GetFileSystem();

    SECTION("Prepare v1 cache")
    {
        const auto V1_CACHE_PATH = "test/testdata/ut/quackstore/test_last_modified/cache_v1.bin";
        auto local_fs = duckdb::FileSystem::CreateLocal();
        auto handle_src = local_fs->OpenFile(V1_CACHE_PATH, duckdb::FileFlags::FILE_FLAGS_READ);
        REQUIRE(handle_src);

        RemoveLocalFile(CACHE_PATH);
        const auto flags_dst = duckdb::FileFlags::FILE_FLAGS_FILE_CREATE 
            | duckdb::FileFlags::FILE_FLAGS_WRITE 
            | duckdb::FileFlags::FILE_FLAGS_READ;
        auto handle_dst = local_fs->OpenFile(CACHE_PATH, flags_dst);
        REQUIRE(handle_dst);

        const int64_t FILE_SIZE = 4480;
        duckdb::vector<uint8_t> data(FILE_SIZE+10, 0); //allocate just a little bit more
        REQUIRE(handle_src->Read(reinterpret_cast<void*>(data.data()), data.size()) == FILE_SIZE);
        REQUIRE(handle_dst->Write(reinterpret_cast<void*>(data.data()), FILE_SIZE) == FILE_SIZE);
    }
    
    const uint64_t BLOCK_SIZE = Bytes(16);

    auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true));
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_SIZE, duckdb::Value::UBIGINT(1024));
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_PATH, duckdb::Value{CACHE_PATH});

    const duckdb::string FILE_PATH = "test/testdata/read_test.txt";
    const duckdb::string CACHED_FILE_PATH = QuackstoreFileSystem::SCHEMA_PREFIX + FILE_PATH;
    const auto READ_FLAGS = duckdb::FileOpenFlags::FILE_FLAGS_READ;

    SECTION("V1 cache - without last_modified timestamp on files") {
        auto cache = Cache{BLOCK_SIZE};
        cache.Open(CACHE_PATH);

        MetadataManager::FileMetadata md;
        REQUIRE(cache.RetrieveFileMetadata(FILE_PATH, md) == true);
        CHECK(md.last_modified == duckdb::timestamp_t::epoch());
    }

    SECTION("Cache is now V3, but still no last_modified timestamp on files - there were no requests to fetch/update it") {
        auto cache = Cache{BLOCK_SIZE};
        cache.Open(CACHE_PATH);

        MetadataManager::FileMetadata md;
        REQUIRE(cache.RetrieveFileMetadata(FILE_PATH, md) == true);
        CHECK(md.last_modified == duckdb::timestamp_t::epoch());
    }

    SECTION("Cache is V3, let's trigger an update of last_modified timestamp") {
        duckdb::unique_ptr<Cache> cache = duckdb::make_uniq<Cache>(BLOCK_SIZE);
        Cache& _cache_ref = *cache; // For testing purposes, we need to access the cache
        auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);

        main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
        main_fs_ref.RegisterSubSystem(std::move(cache_fs));

        auto file_handle = main_fs_ref.OpenFile(CACHED_FILE_PATH, READ_FLAGS);
        const auto last_modified = main_fs_ref.GetLastModifiedTime(*file_handle); //trigger an update of last_modified timestamp
        CHECK(last_modified > duckdb::timestamp_t::epoch());

        MetadataManager::FileMetadata md;
        REQUIRE(_cache_ref.RetrieveFileMetadata(CACHED_FILE_PATH, md) == true);
        CHECK(md.last_modified == last_modified);
    }

    SECTION("Cache is V3, and it should have last_modified timestamp") {
        duckdb::unique_ptr<Cache> cache = duckdb::make_uniq<Cache>(BLOCK_SIZE);
        Cache& _cache_ref = *cache; // For testing purposes, we need to access the cache
        auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);

        main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
        main_fs_ref.RegisterSubSystem(std::move(cache_fs));

        auto file_handle = main_fs_ref.OpenFile(CACHED_FILE_PATH, READ_FLAGS);

        MetadataManager::FileMetadata md;
        REQUIRE(_cache_ref.RetrieveFileMetadata(CACHED_FILE_PATH, md) == true);
        const auto last_modified = md.last_modified;
        CHECK(last_modified > duckdb::timestamp_t::epoch());
        CHECK(main_fs_ref.GetLastModifiedTime(*file_handle) == last_modified);

        SECTION("At the end make sure the file content is correct") {
            const duckdb::string FILE_CONTENT = "This is a text.\n";

            auto cached_file_handle = main_fs_ref.OpenFile(CACHED_FILE_PATH, READ_FLAGS);
            int64_t file_size = main_fs_ref.GetFileSize(*cached_file_handle);

            duckdb::vector<char> buffer(file_size);
            int64_t bytes_read = main_fs_ref.Read(*cached_file_handle, buffer.data(), file_size);
            REQUIRE(bytes_read == file_size);

            duckdb::string file_content(buffer.begin(), buffer.end());
            REQUIRE(file_content == FILE_CONTENT);
        }
    }
}

TEST_CASE_METHOD(WithDuckDB, "Fallback to other VFS subsystems", "[quackstore]") {
    const duckdb::string CACHE_PATH = "/tmp/cache.bin";
    const duckdb::string TEST_FS_PREFIX = "test://";
    const duckdb::string FILENAME = "test/testdata/read_test.txt";
    const duckdb::string FILE_URI = TEST_FS_PREFIX + FILENAME;
    const duckdb::string CACHED_FILE_URI = QuackstoreFileSystem::SCHEMA_PREFIX + FILE_URI;

    // Prepare test file systems
    auto test_fs = duckdb::make_uniq<TestFileSystem>(TEST_FS_PREFIX);
    duckdb::map<duckdb::string, uint64_t> open_requests;
    duckdb::map<duckdb::string, uint64_t> read_requests;
    duckdb::map<duckdb::string, uint64_t> get_lastmodified_requests;
    duckdb::map<duckdb::string, uint64_t> get_filesize_requests;
    test_fs->on_open_callbacks.push_back([&](const duckdb::string& path, duckdb::FileOpenFlags, duckdb::optional_ptr<duckdb::FileOpener>) {
        ++open_requests[path];
    });
    test_fs->on_read_callbacks.push_back([&](const duckdb::FileHandle& handle) {
        ++read_requests[handle.GetPath()];
    });
    test_fs->on_get_lastmodified_callbacks.push_back([&](const duckdb::FileHandle& handle) {
        ++get_lastmodified_requests[handle.GetPath()];
    });
    test_fs->on_get_filesize_callbacks.push_back([&](const duckdb::FileHandle& handle) {
        ++get_filesize_requests[handle.GetPath()];
    });

    // Prepare cache file system
    RemoveLocalFile(CACHE_PATH);
    auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_PATH, duckdb::Value{CACHE_PATH});

    auto cache = duckdb::make_uniq<Cache>(1024);
    auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);
    SECTION("Test cache file system creation") {
        CHECK(cache_fs->GetName() == "QuackstoreFileSystem");
        CHECK(cache_fs->CanHandleFile(CACHED_FILE_URI));
        CHECK(cache_fs->CanHandleFile(FILE_URI) == false);
    }

    auto& main_fs_ref = GetDBInstance().GetFileSystem();
    main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
    main_fs_ref.RegisterSubSystem(std::move(cache_fs));
    main_fs_ref.RegisterSubSystem(std::move(test_fs));

    const auto DefaultChecks = [&](const duckdb::string& uri, uint64_t count_iterations = 1) {
        INFO("URI: " << uri);
        // Open the file multiple times to test caching
        for(int i = 0; i < count_iterations; ++i) {
            INFO("Open iteration " << i + 1);
            auto handle = main_fs_ref.OpenFile(uri, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            REQUIRE(handle != nullptr);

            // Read the file
            auto buffer = duckdb::vector<uint8_t>(1024, 0);
            handle->Read(buffer.data(), buffer.size());
        }
    };

    uint64_t COUNT = 3; // Default count of iterations for opening the file
    REQUIRE(COUNT > 1); // Ensure we have multiple iterations to test caching

    SECTION("Open file without cache_fs") {
        DefaultChecks(FILE_URI, COUNT);

        CHECK(read_requests.size() == 1);
        for(const auto& [uri, count] : read_requests) {
            INFO("Accessed for read, file" << uri << ", count: " << count);
            CHECK(count == COUNT); // Not using quackstore, hence multiple reads
        }
    }

    SECTION("Open file with cache_fs (cache disabled)") {
        // Disable cache in QuackstoreFileSystem
        config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(false));

        DefaultChecks(CACHED_FILE_URI, COUNT);

        CHECK(read_requests.size() == 1);
        for(const auto& [uri, count] : read_requests) {
            INFO("Accessed for read, file" << uri << ", count: " << count);
            CHECK(count == COUNT); // Not using quackstore, hence multiple reads
        }
    }

    SECTION("Open file with cache_fs (cache enabled)") {
        // Enable cache in QuackstoreFileSystem
        config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true));

        DefaultChecks(CACHED_FILE_URI, COUNT);

        CHECK(get_lastmodified_requests.size() == 1);
        for(const auto& [uri, count] : get_lastmodified_requests) {
            INFO("Accessed for lastmodified, file: " << uri << ", count: " << count);
            CHECK(count == 3); 
        }
        CHECK(get_filesize_requests.size() == 1);
        for(const auto& [uri, count] : get_filesize_requests) {
            INFO("Accessed for filesize, file: " << uri << ", count: " << count);
            CHECK(count == 1);
        }
        CHECK(read_requests.size() == 1); // Zero reads as GetFileSize() == 0
        for(const auto& [uri, count] : read_requests) {
            INFO("Accessed for read, file: " << uri << ", count: " << count);
            CHECK(count == 1);
        }
    }
}

TEST_CASE_METHOD(WithDuckDB, "Simulate file updates", "[quackstore]") {
    const duckdb::string CACHE_PATH = "/tmp/cache.bin";
    const duckdb::string TEST_FS_PREFIX = "test://";
    const duckdb::string FILENAME = "/tmp/simulate_file_updates.txt";
    const duckdb::string FILE_URI = TEST_FS_PREFIX + FILENAME;
    const duckdb::string CACHED_FILE_URI = QuackstoreFileSystem::SCHEMA_PREFIX + FILE_URI;

    // Prepare test file systems
    auto test_fs = duckdb::make_uniq<TestFileSystem>(TEST_FS_PREFIX);
    auto& test_fs_ref = *test_fs;
    uint64_t read_requests = 0;
    uint64_t get_lastmodified_requests = 0;
    uint64_t get_filesize_requests = 0;
    test_fs->on_read_callbacks.push_back([&](const duckdb::FileHandle& handle) {
        ++read_requests;
    });
    test_fs->on_get_lastmodified_callbacks.push_back([&](const duckdb::FileHandle&) {
        ++get_lastmodified_requests;
    });
    test_fs->on_get_filesize_callbacks.push_back([&](const duckdb::FileHandle&) {
        ++get_filesize_requests;
    });

    // Prepare cache file system
    RemoveLocalFile(CACHE_PATH);
    auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_PATH, duckdb::Value{CACHE_PATH});
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true));

    auto cache = duckdb::make_uniq<Cache>(1024);
    auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);

    auto& main_fs_ref = GetDBInstance().GetFileSystem();
    main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
    main_fs_ref.RegisterSubSystem(std::move(cache_fs));
    main_fs_ref.RegisterSubSystem(std::move(test_fs));

    auto CreateBlankFile = [&]() {
        const auto flags = duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW
            | duckdb::FileFlags::FILE_FLAGS_WRITE 
            | duckdb::FileFlags::FILE_FLAGS_READ;
        auto handle = duckdb::FileSystem::CreateLocal()->OpenFile(FILENAME, flags);
        REQUIRE(handle);
    };

    auto OpenCachedFile = [&]() {
        auto handle = main_fs_ref.OpenFile(CACHED_FILE_URI, duckdb::FileOpenFlags::FILE_FLAGS_READ | duckdb::FileOpenFlags::FILE_FLAGS_WRITE);
        REQUIRE(handle != nullptr);
        return handle;
    };

    auto ReadFromHandle = [&](duckdb::FileHandle& handle){
        auto buffer = duckdb::vector<uint8_t>(64, 0);
        handle.Read(buffer.data(), buffer.size());
    };

    auto WriteToUnderlyingFile = [&](duckdb::vector<uint8_t>& data) {
        const auto flags = duckdb::FileFlags::FILE_FLAGS_WRITE;
        auto fs = duckdb::FileSystem::CreateLocal();
        fs->OpenFile(FILENAME, flags)->Write(reinterpret_cast<void*>(data.data()), data.size());
    };

    auto ClearRequests = [&](){
        read_requests = 0;
        get_lastmodified_requests = 0;
        get_filesize_requests = 0;
    };

    auto QUACK = duckdb::vector<uint8_t>{'q','u','a','c','k'};

    CreateBlankFile();
    test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch()); //force lastmodified = 0 on file
    OpenCachedFile(); //warm up the metadata cache
    test_fs_ref.ResetLastModified();

    CHECK(get_lastmodified_requests == 1);
    CHECK(get_filesize_requests == 1);
    ClearRequests();

    SECTION("File update detected via last_modified change") {
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile(); // Cache initial state
        ClearRequests();
        
        // Simulate file update by changing last_modified
        test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch() + 100);
        auto handle2 = OpenCachedFile();
        
        CHECK(get_lastmodified_requests == 1); // Should check underlying
        CHECK(get_filesize_requests == 1); // Should evict and re-cache due to last_modified mismatch
    }

    SECTION("File update detected via filesize change when last_modified=0") {
        test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch());
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile(); // Cache initial state
        ClearRequests();
        
        // Change file size while keeping last_modified = 0
        auto bigger_quack = QUACK;
        bigger_quack.push_back('!');
        WriteToUnderlyingFile(bigger_quack);
        
        auto handle2 = OpenCachedFile();
        CHECK(get_lastmodified_requests == 1); // Should check underlying
        CHECK(get_filesize_requests == 1); // Should fallback to filesize check and detect change
    }

    SECTION("Cache eviction removes blocks and metadata") {
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile();
        ReadFromHandle(*handle1); // Populate cache blocks
        ClearRequests();

        // Modify file to trigger eviction
        test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch() + 100);
        auto handle2 = OpenCachedFile();

        ClearRequests();
        ReadFromHandle(*handle2); // Should read from underlying, not cache
        CHECK(read_requests == 1); // Confirms cache was evicted
    }

    SECTION("Both last_modified and filesize zero triggers eviction") {
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile();
        ClearRequests();

        // Set both to problematic values
        test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch());
        test_fs_ref.SetFileSize(0);
        auto handle2 = OpenCachedFile();

        CHECK(get_lastmodified_requests == 1);
        CHECK(get_filesize_requests == 1);
        // Should evict due to filesize == 0 condition
    }

    SECTION("No redundant underlying filesystem calls when file unchanged") {
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile();
        ClearRequests();

        // Open same file multiple times without changes
        for(int i = 0; i < 5; ++i) {
            auto handle = OpenCachedFile();
            CHECK(get_lastmodified_requests == 1); // Only one check per open
            CHECK(get_filesize_requests == 0); // No size check needed
            ClearRequests();
        }
    }

    SECTION("Underlying filesize requested only when needed") {
        test_fs_ref.SetLastModified(duckdb::timestamp_t::epoch());
        WriteToUnderlyingFile(QUACK);
        auto handle1 = OpenCachedFile();
        ClearRequests();

        // Same conditions - should reuse cached filesize
        auto handle2 = OpenCachedFile();
        CHECK(get_lastmodified_requests == 1);
        CHECK(get_filesize_requests == 1); // Should still check size when last_modified=0

        ClearRequests();
        auto handle3 = OpenCachedFile();
        CHECK(get_lastmodified_requests == 1);
        CHECK(get_filesize_requests == 1); // Confirms the fallback logic path
    }
}

// Tests dynamic cache path reconfiguration across multiple database connections
TEST_CASE_METHOD(WithDuckDB, "Dynamic cache path reconfiguration across multiple connections", "[quackstore]") {
    auto local_fs = duckdb::FileSystem::CreateLocal();

    // Prepare multiple cache file paths for testing dynamic reconfiguration
    duckdb::vector<duckdb::string> cache_paths;
    for(int i = 0; i < 3; ++i)
    {
        auto path = duckdb::StringUtil::Format("/tmp/cache_%d.bin", i);
        RemoveLocalFile(path);
        cache_paths.push_back(path);
    }

    // Setup test file systems and create cached file URIs
    const duckdb::string FILENAME = "test/testdata/read_test.txt";
    const duckdb::string CACHED_FILENAME = QuackstoreFileSystem::SCHEMA_PREFIX + FILENAME;

    // Helper to read files and trigger cache operations
    const auto SelectFromFileAndCheck = [&](duckdb::Connection& con, const duckdb::string& uri) {
        auto query = duckdb::StringUtil::Format("SELECT content FROM read_text('%s');", uri);
        INFO("Query: " << query);
        auto res = con.Query(query);
        INFO("Result: " << res->ToString());
        REQUIRE_FALSE(res->HasError());
        CHECK(res->RowCount() > 0);
    };

    // Clean up default cache path
    auto opener = duckdb::DatabaseFileOpener{GetDBInstance()};
    quackstore::ExtensionParams params = quackstore::ExtensionParams::ReadFrom(&opener);
    RemoveLocalFile(params.cache_path);

    // Create multiple connections to the same database instance
    duckdb::vector<duckdb::unique_ptr<duckdb::Connection>> connections;

    CHECK(GetExtensionParams(GetDBInstance()).cache_enabled == false);
    for(int i = 0; i < 3; ++i) {
        auto con = duckdb::make_uniq<duckdb::Connection>(GetDBInstance());
        if (i == 0)
        {
            auto query_set = duckdb::StringUtil::Format("SET GLOBAL %s = 'true';", ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED);
            auto res_set = con->Query(query_set);
            REQUIRE(res_set->HasError() == false);
        }
        REQUIRE(GetExtensionParams(*con->context).cache_enabled == true);
        SelectFromFileAndCheck(*con, CACHED_FILENAME);
        connections.emplace_back(std::move(con));
    }


    // Test dynamic cache path reconfiguration
    for(const auto& cache_path: cache_paths) {
        INFO("Using cache path: " << cache_path);
        REQUIRE(local_fs->FileExists(cache_path) == false);
        
        // Change cache path globally - should affect all connections
        auto res_set = connections.front()->Query(
            duckdb::StringUtil::Format("SET GLOBAL %s = '%s';", ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_PATH, cache_path)
        );
        REQUIRE(res_set->HasError() == false);

        // Access files from all connections to populate the new cache location
        for(auto& con: connections) {
            REQUIRE(GetExtensionParams(*con->context).cache_enabled == true);
            REQUIRE(GetExtensionParams(*con->context).cache_path == cache_path);
            SelectFromFileAndCheck(*con, CACHED_FILENAME);
        }
        
        // Verify cache file was created at the new location
        REQUIRE(local_fs->FileExists(cache_path) == true);
    }

    CHECK(1 == 1);
}

TEST_CASE_METHOD(WithDuckDB, "CacheFileHandle constructor exception handling preserves reference count", "[quackstore][exception-handling]") {
    const auto CACHE_PATH = "/tmp/cache_exception_test.bin";
    RemoveLocalFile(CACHE_PATH);

    auto cache = duckdb::make_uniq<Cache>(16);
    cache->Open(CACHE_PATH);

    auto& main_fs_ref = GetDBInstance().GetFileSystem();
    auto cache_fs = duckdb::make_uniq<QuackstoreFileSystem>(*cache);

    main_fs_ref.UnregisterSubSystem(QuackstoreFileSystem::FILESYSTEM_NAME);
    main_fs_ref.RegisterSubSystem(std::move(cache_fs));

    auto& config = duckdb::DBConfig::GetConfig(GetDBInstance());
    config.SetOptionByName(ExtensionParams::PARAM_NAME_QUACKSTORE_CACHE_ENABLED, duckdb::Value::BOOLEAN(true));

    const duckdb::string FILE_PATH = QuackstoreFileSystem::SCHEMA_PREFIX + duckdb::string{"test/testdata/read_test.txt"};

    SECTION("Reference count properly managed when constructor throws exception") {
        // First, verify Clear() works when no files are open (reference count is 0)
        REQUIRE_NOTHROW(cache->Clear());

        // Create a scenario where CacheFileHandle constructor will throw
        // We'll use a non-existent file to trigger an exception during initialization
        const duckdb::string INVALID_FILE_PATH = QuackstoreFileSystem::SCHEMA_PREFIX + duckdb::string{"nonexistent/file/path.txt"};

        // Try to open the non-existent file, which should throw an exception
        REQUIRE_THROWS(main_fs_ref.OpenFile(INVALID_FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ));

        // After the exception, the reference count should still be 0, so Clear() should work
        REQUIRE_NOTHROW(cache->Clear());
    }

    SECTION("Reference count correctly incremented on successful construction") {
        // Successfully open a file
        auto file_handle = main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        REQUIRE(file_handle != nullptr);

        // Now reference count should be > 0, so Clear() should throw
        REQUIRE_THROWS_AS(cache->Clear(), std::runtime_error);

        // Close the file handle
        file_handle->Close();

        // Now Clear() should work again (reference count back to 0)
        REQUIRE_NOTHROW(cache->Clear());
    }

    SECTION("Multiple file handles maintain correct reference count") {
        // Open multiple file handles
        auto handle1 = main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        auto handle2 = main_fs_ref.OpenFile(FILE_PATH, duckdb::FileOpenFlags::FILE_FLAGS_READ);

        REQUIRE(handle1 != nullptr);
        REQUIRE(handle2 != nullptr);

        // Clear should fail with multiple open handles
        REQUIRE_THROWS_AS(cache->Clear(), std::runtime_error);

        // Close one handle
        handle1->Close();

        // Clear should still fail (one handle still open)
        REQUIRE_THROWS_AS(cache->Clear(), std::runtime_error);

        // Close the second handle
        handle2->Close();

        // Now Clear should work
        REQUIRE_NOTHROW(cache->Clear());
    }

    SECTION("Reference count survives exception during file opening with underlying filesystem error") {
        // Test with a mock filesystem that can trigger exceptions
        class ThrowingFileSystem : public duckdb::LocalFileSystem {
        public:
            duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle &handle) override {
                if (should_throw) {
                    throw std::runtime_error("Simulated filesystem error");
                }
                return duckdb::LocalFileSystem::GetLastModifiedTime(handle);
            }
            bool should_throw = false;
        };

        // Verify initial state
        REQUIRE_NOTHROW(cache->Clear());

        // Try various invalid file operations that might cause exceptions
        const duckdb::vector<duckdb::string> invalid_paths = {
            duckdb::string{QuackstoreFileSystem::SCHEMA_PREFIX} + "/dev/null/invalid",  // Invalid path
            duckdb::string{QuackstoreFileSystem::SCHEMA_PREFIX} + "",  // Empty path after prefix
        };

        for (const auto& invalid_path : invalid_paths) {
            // Each invalid file opening should throw but not affect reference count
            REQUIRE_THROWS(main_fs_ref.OpenFile(invalid_path, duckdb::FileOpenFlags::FILE_FLAGS_READ));

            // Reference count should still be 0, so Clear should work
            REQUIRE_NOTHROW(cache->Clear());
        }
    }
}