#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/connection.hpp>

#include "cache.hpp"
#include "cachefs_extension.hpp"

using namespace duckdb;

TEST_CASE("ExecClearCacheFunction removes cache file when cache was not created and not opened", "[ClearCacheFunction]") {
    string cache_path = "/tmp/test_exec_clear_opened.bin";
    auto local_fs = FileSystem::CreateLocal();
    if (local_fs->FileExists(cache_path)) {
        local_fs->RemoveFile(cache_path);
    }

    DuckDB db(nullptr);
    Connection con(db);

    // Configure cache
    auto result = con.Query("SET GLOBAL cachefs_cache_path = '" + cache_path + "'");
    REQUIRE_FALSE(result->HasError());
    result = con.Query("SET GLOBAL cachefs_cache_enabled = true");
    REQUIRE_FALSE(result->HasError());

    // Verify cache file doesn't exist initially
    REQUIRE_FALSE(local_fs->FileExists(cache_path));

    // Clear cache (should handle non-existent cache gracefully)
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());

    // Still shouldn't exist
    REQUIRE_FALSE(local_fs->FileExists(cache_path));
}

TEST_CASE("ExecClearCacheFunction removes cache file when cache was created but not opened", "[ClearCacheFunction]") {
    string cache_path = "/tmp/test_exec_clear_unopened.bin";
    auto local_fs = FileSystem::CreateLocal();
    if (local_fs->FileExists(cache_path)) {
        local_fs->RemoveFile(cache_path);
    }

    DuckDB db(nullptr);
    Connection con(db);

    // Configure cache
    auto result = con.Query("SET GLOBAL cachefs_cache_path = '" + cache_path + "'");
    REQUIRE_FALSE(result->HasError());
    result = con.Query("SET GLOBAL cachefs_cache_enabled = true");
    REQUIRE_FALSE(result->HasError());

    // Manually create a cache file to simulate a previously created cache
    {
        auto cache = cachefs::Cache(CachefsExtension::BLOCK_SIZE);
        cache.Open(cache_path);
        vector<uint8_t> data(1024 * 64, 'x');
        cache.StoreBlock("/test/file.txt", 0, data);
        cache.Flush();
        cache.Close();
    }

    REQUIRE(local_fs->FileExists(cache_path));

    // Call cachefs_clear_cache - it should open the cache and then clear it
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());

    // Verify the result indicates success
    REQUIRE(result->RowCount() == 1);
    auto success_value = result->GetValue(0, 0);
    REQUIRE(success_value.GetValue<bool>() == true);

    // Cache file should be removed
    REQUIRE_FALSE(local_fs->FileExists(cache_path));
}

TEST_CASE("ExecClearCacheFunction handles multiple consecutive calls", "[ClearCacheFunction]") {
    string cache_path = "/tmp/test_exec_clear_multiple.bin";
    auto local_fs = FileSystem::CreateLocal();
    if (local_fs->FileExists(cache_path)) {
        local_fs->RemoveFile(cache_path);
    }

    DuckDB db(nullptr);
    Connection con(db);

    // Configure cache
    auto result = con.Query("SET GLOBAL cachefs_cache_path = '" + cache_path + "'");
    REQUIRE_FALSE(result->HasError());
    result = con.Query("SET GLOBAL cachefs_cache_enabled = true");
    REQUIRE_FALSE(result->HasError());

    // Create initial cache file
    {
        auto cache = cachefs::Cache(CachefsExtension::BLOCK_SIZE);
        cache.Open(cache_path);
        vector<uint8_t> data(1024 * 64, 'a');
        cache.StoreBlock("/test/file1.txt", 0, data);
        cache.Flush();
        cache.Close();
    }

    REQUIRE(local_fs->FileExists(cache_path));

    // First clear
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);
    REQUIRE_FALSE(local_fs->FileExists(cache_path));

    // Second clear on non-existent cache
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);
    REQUIRE_FALSE(local_fs->FileExists(cache_path));

    // Create cache again
    {
        auto cache = cachefs::Cache(CachefsExtension::BLOCK_SIZE);
        cache.Open(cache_path);
        vector<uint8_t> data(1024 * 64, 'b');
        cache.StoreBlock("/test/file2.txt", 0, data);
        cache.Flush();
        cache.Close();
    }

    REQUIRE(local_fs->FileExists(cache_path));

    // Third clear
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);
    REQUIRE_FALSE(local_fs->FileExists(cache_path));
}

TEST_CASE("ExecClearCacheFunction handles exception gracefully", "[ClearCacheFunction]") {
    DuckDB db(nullptr);
    Connection con(db);

    // Set an invalid cache path to potentially cause an exception
    auto result = con.Query("SET GLOBAL cachefs_cache_path = ''");
    REQUIRE_FALSE(result->HasError());
    result = con.Query("SET GLOBAL cachefs_cache_enabled = true");
    REQUIRE_FALSE(result->HasError());

    // Clear should handle the exception and return false
    result = con.Query("SELECT * FROM cachefs_clear_cache()");
    REQUIRE_FALSE(result->HasError());
    REQUIRE(result->RowCount() == 1);

    // Should return false due to exception (empty path is invalid)
    auto success_value = result->GetValue(0, 0);
    REQUIRE(success_value.GetValue<bool>() == false);
}
