#pragma once

#include "duckdb.hpp"

namespace duckdb {
    class FileOpener;
    class ClientContext;
    class DatabaseInstance;
    struct DBConfig;
}

namespace quackstore {

struct ExtensionParams {
    static constexpr const auto PARAM_NAME_QUACKSTORE_CACHE_ENABLED = "quackstore_cache_enabled";
    static constexpr bool DEFAULT_QUACKSTORE_CACHE_ENABLED = false;
    bool cache_enabled = DEFAULT_QUACKSTORE_CACHE_ENABLED;

    static constexpr const auto PARAM_NAME_QUACKSTORE_CACHE_SIZE = "quackstore_cache_size";
    static constexpr uint64_t DEFAULT_QUACKSTORE_CACHE_SIZE = 2ULL * 1024 * 1024 * 1024; // 2 GB
    uint64_t max_cache_size = DEFAULT_QUACKSTORE_CACHE_SIZE;

    static constexpr const auto PARAM_NAME_QUACKSTORE_CACHE_PATH = "quackstore_cache_path";
    static constexpr const char* DEFAULT_QUACKSTORE_CACHE_PATH = "/tmp/duckdb_block_cache.bin";
    duckdb::string cache_path = DEFAULT_QUACKSTORE_CACHE_PATH;

    static constexpr const auto PARAM_NAME_QUACKSTORE_DATA_MUTABLE = "quackstore_data_mutable";
    static constexpr bool DEFAULT_QUACKSTORE_DATA_MUTABLE = true;
    bool data_mutable = DEFAULT_QUACKSTORE_DATA_MUTABLE;

    static ExtensionParams ReadFrom(duckdb::optional_ptr<duckdb::FileOpener> opener);
    static ExtensionParams ReadFrom(const duckdb::ClientContext& context);
    static ExtensionParams ReadFrom(const duckdb::DatabaseInstance& instance);
    static void AddExtensionOptions(duckdb::DBConfig &config);
};

}  // namespace quackstore
