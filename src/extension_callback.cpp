#include <duckdb.hpp>
#include <duckdb/main/client_data.hpp>

#include "extension_callback.hpp"
#include "cache.hpp"
#include "extension_state.hpp"

namespace cachefs {

ExtensionCallback::ExtensionCallback(duckdb::unique_ptr<Cache> in_cache) 
: cache(std::move(in_cache)) 
{}

void ExtensionCallback::OnConnectionOpened(duckdb::ClientContext &context) 
{
    auto& cache_ref = *cache;

    context.registered_state->Insert(ExtensionState::EXTENSION_STATE_NAME, duckdb::make_shared_ptr<ExtensionState>(cache_ref));
}

} // namespace cachefs