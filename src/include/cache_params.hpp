#pragma once

#include "duckdb.hpp"

namespace duckdb {
    class FileOpener;
    class ClientContext;
    class DatabaseInstance;
    struct DBConfig;
}

namespace cachefs {

struct ExtensionParams {
    static constexpr const auto PARAM_NAME_CACHEFS_CACHE_ENABLED = "cachefs_cache_enabled";
    static constexpr bool DEFAULT_CACHEFS_CACHE_ENABLED = false;
    bool cache_enabled = DEFAULT_CACHEFS_CACHE_ENABLED;

    static constexpr const auto PARAM_NAME_CACHEFS_CACHE_SIZE = "cachefs_cache_size";
    static constexpr uint64_t DEFAULT_CACHEFS_CACHE_SIZE = 2ULL * 1024 * 1024 * 1024; // 2 GB
    uint64_t max_cache_size = DEFAULT_CACHEFS_CACHE_SIZE;

    static constexpr const auto PARAM_NAME_CACHEFS_CACHE_PATH = "cachefs_cache_path";
    static constexpr const char* DEFAULT_CACHEFS_CACHE_PATH = "/tmp/duckdb_block_cache.bin";
    duckdb::string cache_path = DEFAULT_CACHEFS_CACHE_PATH;

    static constexpr const auto PARAM_NAME_CACHEFS_DATA_MUTABLE = "cachefs_data_mutable";
    static constexpr bool DEFAULT_CACHEFS_DATA_MUTABLE = true;
    bool data_mutable = DEFAULT_CACHEFS_DATA_MUTABLE;

    static ExtensionParams ReadFrom(duckdb::optional_ptr<duckdb::FileOpener> opener);
    static ExtensionParams ReadFrom(const duckdb::ClientContext& context);
    static ExtensionParams ReadFrom(const duckdb::DatabaseInstance& instance);
    static void AddExtensionOptions(duckdb::DBConfig &config);
};

}  // namespace cachefs
