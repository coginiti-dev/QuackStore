#include "cache_params.hpp"

#include <duckdb/common/file_opener.hpp>

#include "extension_state.hpp"
#include "cache_file_system.hpp"
#include "cache.hpp"

namespace
{
    void ValidateGlobalScope(duckdb::SetScope scope)
    {
        if (scope != duckdb::SetScope::GLOBAL) {
            throw duckdb::CatalogException("Cache file system parameters can only be set globally");
        }
    }

    void callback_set_cache_enabled(duckdb::ClientContext& context, duckdb::SetScope scope, duckdb::Value& value)
    {
        ValidateGlobalScope(scope);
    }    
    void callback_set_cache_size(duckdb::ClientContext& context, duckdb::SetScope scope, duckdb::Value& value)
    {
        ValidateGlobalScope(scope);

        auto val = value.GetValue<uint64_t>();

        auto state_ptr = cachefs::ExtensionState::RetrieveFromContext(context);
        if (!state_ptr) {
            throw duckdb::InternalException("Cache file system state is not initialized");
        }
        state_ptr->GetCache().SetMaxCacheSize(val);
    }    
    void callback_set_cache_path(duckdb::ClientContext& context, duckdb::SetScope scope, duckdb::Value& value)
    {
        ValidateGlobalScope(scope);

        auto state_ptr = cachefs::ExtensionState::RetrieveFromContext(context);
        if (!state_ptr) {
            throw duckdb::InternalException("Cache file system state is not initialized");
        }
        auto& cache = state_ptr->GetCache();
        auto new_path = value.GetValue<duckdb::string>();
        if (new_path == cache.GetPath())
        {
            return;
        }

        cache.Close();
    }
}

namespace cachefs {

// =============================================================================
// ExtensionParams
// =============================================================================

ExtensionParams ExtensionParams::ReadFrom(duckdb::optional_ptr<duckdb::FileOpener> opener) {
    ExtensionParams result;

    duckdb::Value value;
    if (duckdb::FileOpener::TryGetCurrentSetting(opener, PARAM_NAME_CACHEFS_CACHE_ENABLED, value)) {
        auto enabled = value.GetValue<bool>();
        result.cache_enabled = enabled;
    }
    if (duckdb::FileOpener::TryGetCurrentSetting(opener, PARAM_NAME_CACHEFS_CACHE_SIZE, value)) {
        auto cache_size_in_bytes = value.GetValue<uint64_t>();
        result.max_cache_size = cache_size_in_bytes;
    }
    if (duckdb::FileOpener::TryGetCurrentSetting(opener, PARAM_NAME_CACHEFS_CACHE_PATH, value)) {
        auto path = value.GetValue<duckdb::string>();
        result.cache_path = path;
    }
    if (duckdb::FileOpener::TryGetCurrentSetting(opener, PARAM_NAME_CACHEFS_DATA_MUTABLE, value)) {
        auto data_mutable = value.GetValue<bool>();
        result.data_mutable = data_mutable;
    }

    return result;
}

ExtensionParams ExtensionParams::ReadFrom(const duckdb::ClientContext& context) {
    ExtensionParams result;

    duckdb::Value value;
    if (context.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_ENABLED, value)) {
        auto enabled = value.GetValue<bool>();
        result.cache_enabled = enabled;
    }
    if (context.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_SIZE, value)) {
        auto cache_size_in_bytes = value.GetValue<uint64_t>();
        result.max_cache_size = cache_size_in_bytes;
    }
    if (context.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_PATH, value)) {
        auto path = value.GetValue<duckdb::string>();
        result.cache_path = path;
    }
    if (context.TryGetCurrentSetting(PARAM_NAME_CACHEFS_DATA_MUTABLE, value)) {
        auto data_mutable = value.GetValue<bool>();
        result.data_mutable = data_mutable;
    }

    return result;
}


ExtensionParams ExtensionParams::ReadFrom(const duckdb::DatabaseInstance& instance) {
    ExtensionParams result;

    duckdb::Value value;
    if (instance.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_ENABLED, value)) {
        auto enabled = value.GetValue<bool>();
        result.cache_enabled = enabled;
    }
    if (instance.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_SIZE, value)) {
        auto cache_size_in_bytes = value.GetValue<uint64_t>();
        result.max_cache_size = cache_size_in_bytes;
    }
    if (instance.TryGetCurrentSetting(PARAM_NAME_CACHEFS_CACHE_PATH, value)) {
        auto path = value.GetValue<duckdb::string>();
        result.cache_path = path;
    }
    if (instance.TryGetCurrentSetting(PARAM_NAME_CACHEFS_DATA_MUTABLE, value)) {
        auto data_mutable = value.GetValue<bool>();
        result.data_mutable = data_mutable;
    }

    return result;
}


void ExtensionParams::AddExtensionOptions(duckdb::DBConfig &config) {
    ExtensionParams default_params;

    config.AddExtensionOption(
        PARAM_NAME_CACHEFS_CACHE_ENABLED, 
        "Turn cache ON or OFF",
        duckdb::LogicalTypeId::BOOLEAN,
        duckdb::Value::BOOLEAN(default_params.cache_enabled),
        callback_set_cache_enabled
    );
    config.AddExtensionOption(
        PARAM_NAME_CACHEFS_CACHE_SIZE, 
        "Cache size (bytes)",
        duckdb::LogicalTypeId::UBIGINT,
        duckdb::Value::UBIGINT(default_params.max_cache_size),
        callback_set_cache_size
    );
    config.AddExtensionOption(
        PARAM_NAME_CACHEFS_CACHE_PATH, 
        "Cache path",
        duckdb::LogicalTypeId::VARCHAR,
        duckdb::Value{default_params.cache_path},
        callback_set_cache_path
    );
    config.AddExtensionOption(
        PARAM_NAME_CACHEFS_DATA_MUTABLE, 
        "Whether data is mutable (affects cache invalidation)",
        duckdb::LogicalTypeId::BOOLEAN,
        duckdb::Value::BOOLEAN(default_params.data_mutable)
    );
}

}  // namespace cachefs
