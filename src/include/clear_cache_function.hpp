#pragma once

#include <duckdb.hpp>

namespace duckdb {
    class ClientContext;
    class TableFunctionSet;
    class ScalarFunctionSet;
}

namespace cachefs {

class Functions
{
public:
	static duckdb::vector<duckdb::TableFunctionSet> GetTableFunctions(duckdb::DatabaseInstance& instance);
};

}  // namespace cachefs
