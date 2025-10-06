#pragma once

#include <duckdb.hpp>

namespace duckdb {
    class ClientContext;
    class TableFunctionSet;
    class ScalarFunctionSet;
}

namespace quackstore {

class Functions
{
public:
	static duckdb::vector<duckdb::TableFunctionSet> GetTableFunctions(duckdb::DatabaseInstance& instance);
};

}  // namespace quackstore
