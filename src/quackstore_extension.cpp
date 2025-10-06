#define DUCKDB_EXTENSION_MAIN

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/virtual_file_system.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/connection_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

#include "quackstore_filesystem.hpp"
#include "cache.hpp"
#include "quackstore_params.hpp"
#include "quackstore_extension.hpp"
#include "extension_callback.hpp"
#include "extension_state.hpp"

namespace {
    const char* EXTENSION_NAME = "quackstore";
}

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    auto& instance = loader.GetDatabaseInstance();

    auto &config = DBConfig::GetConfig(instance);
    quackstore::ExtensionParams::AddExtensionOptions(config);

    // NOTE: Cache is initialized here but will be lazily opened in the cache file system when first file is opened.
    unique_ptr<quackstore::Cache> cache = make_uniq<quackstore::Cache>(QuackstoreExtension::BLOCK_SIZE);

    // Register block caching file system
    instance.GetFileSystem().RegisterSubSystem(make_uniq<quackstore::QuackstoreFileSystem>(*cache));

    // Register extension functions
	for (auto& fun : quackstore::Functions::GetTableFunctions(instance)) {
    	auto info = CreateTableFunctionInfo{fun};
	    info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		loader.RegisterFunction(std::move(info));
	}

    auto extension_callback = make_uniq<quackstore::ExtensionCallback>(std::move(cache));
    for (auto& connection : ConnectionManager::Get(instance).GetConnectionList()) {
        extension_callback->OnConnectionOpened(*connection);
    }
    config.extension_callbacks.push_back(std::move(extension_callback));
}

void QuackstoreExtension::Load(ExtensionLoader &loader) { 
    LoadInternal(loader); 
}

string QuackstoreExtension::Name() { 
    return EXTENSION_NAME; 
}

}  // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(quackstore, loader) {
	duckdb::LoadInternal(loader);
}

// DUCKDB_EXTENSION_API const char *quackstore_version() { return duckdb::DuckDB::LibraryVersion(); }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
