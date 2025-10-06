PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=cachefs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;icu'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Custom makefile targets

#alias for clean
.PHONY: clear
clear: clean 
	@echo > /dev/null

###
### Run duckdb CLI
###
.PHONY: duckdb/debug
duckdb/debug:
	./build/debug/duckdb

.PHONY: duckdb/release
duckdb/release:
	./build/release/duckdb

###
### Run unit (Catch2) tests
###
.PHONY: test/unit/debug
test/unit/debug:
	./build/debug/extension/cachefs/test/unittest/unittest_cachefs

.PHONY: test/unit/release
test/unit/release:
	./build/release/extension/cachefs/test/unittest/unittest_cachefs

###
### Run SQL tests
###
# see test_debug / test_release targets in the included Makefile from extension-ci-tools


###
### Run unit and SQL tests
###
.PHONY: test/cachefs/debug
test/cachefs/debug: test/unit/debug test_debug
	@echo > /dev/null

.PHONY: test/cachefs/release
test/cachefs/release: test/unit/release test_release
	@echo > /dev/null
