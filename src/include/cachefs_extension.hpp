#ifndef INCLUDE_DUCKDB_WEB_EXTENSIONS_CACHEFS_EXTENSION_H_
#define INCLUDE_DUCKDB_WEB_EXTENSIONS_CACHEFS_EXTENSION_H_

#include <duckdb.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/extension_callback.hpp>

#include "clear_cache_function.hpp"

namespace duckdb {

class CachefsExtension : public Extension {
public:
    static constexpr uint64_t BLOCK_SIZE = 1ULL * 1024 * 1024;  // 1 MB
    void Load(DuckDB &db) override;
    string Name() override;
};

}  // namespace duckdb

extern "C" void cachefs_init(duckdb::DatabaseInstance &db);

#endif
