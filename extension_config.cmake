# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(quackstore
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Add more extensions here, e.g.
# duckdb_extension_load(json)
# duckdb_extension_load(httpfs)
# duckdb_extension_load(icu)
