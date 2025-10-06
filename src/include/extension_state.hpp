#pragma once

#include <duckdb.hpp>

namespace duckdb {
    class ClientContext;
}

namespace quackstore {

class QuackstoreFileSystem;
class Cache;

class ExtensionState : public duckdb::ClientContextState {
public:
    static const duckdb::string EXTENSION_STATE_NAME;

    static duckdb::shared_ptr<ExtensionState> RetrieveFromContext(duckdb::ClientContext& context);

    ExtensionState(Cache& cache);

    Cache& GetCache() const;

private:
    Cache& cache;
};

}