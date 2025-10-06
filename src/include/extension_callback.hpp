#pragma once
#include <duckdb/planner/extension_callback.hpp>

namespace duckdb {
    class ClientContext;
}

namespace quackstore {

class Cache;

class ExtensionCallback : public duckdb::ExtensionCallback 
{
public:
    ExtensionCallback(duckdb::unique_ptr<Cache> cache);
    void OnConnectionOpened(duckdb::ClientContext &context) override;

private:
    duckdb::unique_ptr<Cache> cache;
};

}  // namespace quackstore