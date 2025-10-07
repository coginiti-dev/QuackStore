#include "extension_state.hpp"
#include <duckdb/main/client_context.hpp>
#include "cache.hpp"


namespace quackstore {

const duckdb::string ExtensionState::EXTENSION_STATE_NAME = "quackstore_extension_state";

ExtensionState::ExtensionState(Cache& cache)
: cache(cache) 
{}

duckdb::shared_ptr<ExtensionState> ExtensionState::RetrieveFromContext(duckdb::ClientContext& context) {
    return context.registered_state->Get<ExtensionState>(ExtensionState::EXTENSION_STATE_NAME);
}

Cache& ExtensionState::GetCache() const {
    return cache;
}

}