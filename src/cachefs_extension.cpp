#define DUCKDB_EXTENSION_MAIN

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/virtual_file_system.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/connection_manager.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

#include "cache_file_system.hpp"
#include "cache.hpp"
#include "cache_params.hpp"
#include "cachefs_extension.hpp"
#include "extension_callback.hpp"
#include "extension_state.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    auto &config = DBConfig::GetConfig(instance);
    cachefs::ExtensionParams::AddExtensionOptions(config);

    // NOTE: Cache is initialized here but will be lazily opened in the cache file system when first file is opened.
    unique_ptr<cachefs::Cache> cache = make_uniq<cachefs::Cache>(CachefsExtension::BLOCK_SIZE);

    // Register block caching file system
    instance.GetFileSystem().RegisterSubSystem(make_uniq<cachefs::CacheFileSystem>(*cache));

    // Register extension functions
	for (auto& fun : cachefs::Functions::GetTableFunctions(instance)) {
    	auto info = CreateTableFunctionInfo{fun};
	    info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		ExtensionUtil::RegisterFunction(instance, std::move(info));
	}

    auto extension_callback = make_uniq<cachefs::ExtensionCallback>(std::move(cache));
    for (auto& connection : ConnectionManager::Get(instance).GetConnectionList()) {
        extension_callback->OnConnectionOpened(*connection);
    }
    config.extension_callbacks.push_back(std::move(extension_callback));
}

void CachefsExtension::Load(DuckDB &db) { 
    LoadInternal(*db.instance); 
}

duckdb::string CachefsExtension::Name() { 
    return "cachefs"; 
}

}  // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void cachefs_init(duckdb::DatabaseInstance &db) { duckdb::LoadInternal(db); }

DUCKDB_EXTENSION_API const char *cachefs_version() { return duckdb::DuckDB::LibraryVersion(); }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
