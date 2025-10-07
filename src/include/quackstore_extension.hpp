#ifndef INCLUDE_DUCKDB_WEB_EXTENSIONS_QUACKSTORE_EXTENSION_H_
#define INCLUDE_DUCKDB_WEB_EXTENSIONS_QUACKSTORE_EXTENSION_H_

#include <duckdb.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/extension_callback.hpp>

#include "quackstore_functions.hpp"

namespace duckdb {

class ExtensionLoader;

class QuackstoreExtension : public Extension {
public:
    static constexpr uint64_t BLOCK_SIZE = 1ULL * 1024 * 1024;  // 1 MB
    void Load(ExtensionLoader &loader) override;
    string Name() override;
};

}  // namespace duckdb

#endif
