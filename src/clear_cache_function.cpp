#include <duckdb.hpp>
#include <duckdb/common/virtual_file_system.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/main/database.hpp>

#include "cache_params.hpp"
#include "clear_cache_function.hpp"
#include "extension_state.hpp"
#include "cache.hpp"

namespace cachefs {

struct ClearCacheFunctionData : public duckdb::TableFunctionData {
    bool finished = false;
};

struct EvictFilesFunctionData : public duckdb::TableFunctionData {
    duckdb::vector<duckdb::string> paths;
    bool finished = false;
};

static duckdb::unique_ptr<duckdb::FunctionData> BindSuccessColumn(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                               duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
    return_types.push_back(duckdb::LogicalType::BOOLEAN);
    names.emplace_back("Success");
    return duckdb::make_uniq<ClearCacheFunctionData>();
}

static duckdb::unique_ptr<duckdb::FunctionData> BindEvictFilesFunction(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                               duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
    return_types.push_back(duckdb::LogicalType::BOOLEAN);
    names.emplace_back("Success");
    
    auto res = duckdb::make_uniq<EvictFilesFunctionData>();

    if (input.inputs.empty()) {
        throw duckdb::BinderException("cachefs_evict_files requires a list of file paths as argument");
    }

    duckdb::Value& list = input.inputs.front();
    
    if (list.IsNull()) {
        throw duckdb::BinderException("cachefs_evict_files argument cannot be NULL");
    }
    
    if (list.type().id() != duckdb::LogicalTypeId::LIST) {
        throw duckdb::BinderException("cachefs_evict_files requires a list argument, got %s", list.type().ToString());
    }
    
    // Check that list elements are strings
    const auto& child_type = duckdb::ListType::GetChildType(list.type());
    if (child_type.id() != duckdb::LogicalTypeId::VARCHAR) {
        throw duckdb::BinderException("cachefs_evict_files requires a list of strings (VARCHAR[]), got %s", list.type().ToString());
    }
    
    const duckdb::vector<duckdb::Value>& values = duckdb::ListValue::GetChildren(list);
    res->paths.reserve(values.size());
    for(const auto& val: values) {
        if (val.IsNull()) {
            throw duckdb::BinderException("cachefs_evict_files list cannot contain NULL values");
        }
        res->paths.emplace_back(duckdb::StringValue::Get(val));
    }

    return std::move(res);
}


static void ExecClearCacheFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p, duckdb::DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
    if (data.finished) {
        return;
    }

    try {
        auto cachefs_state = ExtensionState::RetrieveFromContext(context);
        if (!cachefs_state) {
            // Set output to indicate failure
            output.SetCardinality(1);
            output.data[0].SetValue(0, false);
            data.finished = true;
            return;
        }

        cachefs::ExtensionParams params;
        params = ExtensionParams::ReadFrom(context);

        cachefs_state->GetCache().Open(params.cache_path);
        cachefs_state->GetCache().Clear();
        // Set output to indicate success
        output.SetCardinality(1);
        output.data[0].SetValue(0, true);
    } catch (...) {
        // Set output to indicate failure
        output.SetCardinality(1);
        output.data[0].SetValue(0, false);
    }
    data.finished = true;
}

static void ExecEvictFilesFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p, duckdb::DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<EvictFilesFunctionData>();
    
    if (data.finished) {
        return;
    }

    auto cachefs_state = ExtensionState::RetrieveFromContext(context);
    if (!cachefs_state) {
        // Set output to indicate failure
        output.SetCardinality(1);
        output.data[0].SetValue(0, false);
        data.finished = true;
        return;
    }
    
    auto& cache = cachefs_state->GetCache();
    bool success = true;
    for(const auto& path: data.paths) {
        try {
            cache.Evict(path);
        } catch (...) {
            success = false;
        }
    }
    
    // Set output to indicate success/failure
    output.SetCardinality(1);
    output.data[0].SetValue(0, success);
    data.finished = true;
}

duckdb::TableFunctionSet GetClearCacheFunctions(duckdb::DatabaseInstance& instance)
{
    auto function_set = duckdb::TableFunctionSet{"cachefs_clear_cache"};
    function_set.AddFunction(duckdb::TableFunction{"cachefs_clear_cache", {}, ExecClearCacheFunction, BindSuccessColumn});
    return function_set;
}

duckdb::TableFunctionSet GetEvictFilesFunctions(duckdb::DatabaseInstance& instance)
{
    auto function_set = duckdb::TableFunctionSet{"cachefs_evict_files"};
    function_set.AddFunction(duckdb::TableFunction{"cachefs_evict_files", {duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}, ExecEvictFilesFunction, BindEvictFilesFunction});
    return function_set;
}

duckdb::vector<duckdb::TableFunctionSet> Functions::GetTableFunctions(duckdb::DatabaseInstance& instance) 
{
    return duckdb::vector<duckdb::TableFunctionSet> {
        GetClearCacheFunctions(instance),
        GetEvictFilesFunctions(instance)
    };
}

}  // namespace cachefs
