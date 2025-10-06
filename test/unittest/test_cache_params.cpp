#include <catch/catch.hpp>
#include <duckdb.hpp>
#include <duckdb/main/client_context_file_opener.hpp>
#include <duckdb/main/database_file_opener.hpp>
#include <duckdb/common/local_file_system.hpp>

#include "cache.hpp"
#include "cache_params.hpp"
#include "extension_state.hpp"
#include "cache_file_system.hpp"

using namespace cachefs;

class WithDuckDB {
public:
    WithDuckDB() 
    : duckdb(":memory:")
    {
    }
    duckdb::DuckDB& GetDuckDB() { return duckdb; }
    duckdb::DatabaseInstance& GetDBInstance() { return *duckdb.instance; }
    bool RemoveLocalFile(const std::string &path) {
        auto fs = duckdb::FileSystem::CreateLocal();
        if (fs->FileExists(path)) {
            fs->RemoveFile(path);
            return true;
        }
        return false;
    }

    static ExtensionParams GetExtensionParams(duckdb::DatabaseInstance &db) {
        auto opener = duckdb::DatabaseFileOpener{db};
        return ExtensionParams::ReadFrom(&opener);
    }

    ExtensionParams GetExtensionParams(duckdb::ClientContext &context) {
        auto opener = duckdb::ClientContextFileOpener{context};
        return ExtensionParams::ReadFrom(&opener);
    }

    private:
    duckdb::DuckDB duckdb;
};

TEST_CASE_METHOD(WithDuckDB, "Check Extension Params (from DBInstance)", "[cachefs]") {
    auto params = GetExtensionParams(GetDBInstance());
    CHECK(params.cache_enabled == ExtensionParams::DEFAULT_CACHEFS_CACHE_ENABLED);
    CHECK(params.max_cache_size == ExtensionParams::DEFAULT_CACHEFS_CACHE_SIZE);
    CHECK(params.cache_path == ExtensionParams::DEFAULT_CACHEFS_CACHE_PATH);
    CHECK(params.data_mutable == ExtensionParams::DEFAULT_CACHEFS_DATA_MUTABLE);
}

TEST_CASE_METHOD(WithDuckDB, "Check Extension Params (from ClientContext)", "[cachefs]") {
    auto connection = duckdb::Connection{GetDBInstance()};
    auto params = GetExtensionParams(*connection.context);
    CHECK(params.cache_enabled == ExtensionParams::DEFAULT_CACHEFS_CACHE_ENABLED);
    CHECK(params.max_cache_size == ExtensionParams::DEFAULT_CACHEFS_CACHE_SIZE);
    CHECK(params.cache_path == ExtensionParams::DEFAULT_CACHEFS_CACHE_PATH);
    CHECK(params.data_mutable == ExtensionParams::DEFAULT_CACHEFS_DATA_MUTABLE);
}

TEST_CASE_METHOD(WithDuckDB, "Set CacheParams programmatically", "[cache_params]") {
    auto &db = GetDBInstance();
    auto &config = db.config;
    auto con = duckdb::Connection{db};

    // Set new values
    for(bool val: {true, false}) {
        config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_CACHE_ENABLED, duckdb::Value::BOOLEAN(val));
        CHECK(GetExtensionParams(db).cache_enabled == val);
        CHECK(GetExtensionParams(*con.context).cache_enabled == val);

        auto another_con = duckdb::Connection{db};
        CHECK(GetExtensionParams(*another_con.context).cache_enabled == val);
    }
    for(uint64_t val: {1024, 1024 * 1024, 1024 * 1024 * 1024}) {
        config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_CACHE_SIZE, duckdb::Value::UBIGINT(val));
        CHECK(GetExtensionParams(db).max_cache_size == val);
        CHECK(GetExtensionParams(*con.context).max_cache_size == val);

        auto another_con = duckdb::Connection{db};
        CHECK(GetExtensionParams(*another_con.context).max_cache_size == val);
    }
    for(duckdb::string val : {"/tmp/test_cache_0", "/tmp/test_cache_1", "/tmp/test_cache_2"}) {
        auto path = val + "_" + std::to_string(std::time(nullptr)) + ".bin";
        config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_CACHE_PATH, duckdb::Value(path));
        CHECK(GetExtensionParams(db).cache_path == path);
        CHECK(GetExtensionParams(*con.context).cache_path == path);

        auto another_con = duckdb::Connection{db};
        CHECK(GetExtensionParams(*another_con.context).cache_path == path);

        RemoveLocalFile(path);
    }
    for(bool val: {true, false}) {
        config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE, duckdb::Value::BOOLEAN(val));
        CHECK(GetExtensionParams(db).data_mutable == val);
        CHECK(GetExtensionParams(*con.context).data_mutable == val);

        auto another_con = duckdb::Connection{db};
        CHECK(GetExtensionParams(*another_con.context).data_mutable == val);
    }
}

TEST_CASE_METHOD(WithDuckDB, "Set CacheParams via SET / SET GLOBAL", "[cache_params]") {
    auto& db = GetDBInstance();
    auto con = duckdb::Connection{db};

    const auto EnsureResultError = [=](duckdb::QueryResult& res) {
        REQUIRE(res.HasError() == true);
        CHECK(res.GetErrorObject().Type() == duckdb::ExceptionType::CATALOG);
        CHECK(res.GetErrorObject().RawMessage() == "Cache file system parameters can only be set globally");
    };

    for(duckdb::string enabled : {"true", "false"}) {
        SECTION("Set Cache Enabled (LOCAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_ENABLED,
                enabled
            );
            auto res = con.Query(query);
            EnsureResultError(*res);
        }

        SECTION("Set Cache Enabled (GLOBAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET GLOBAL %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_ENABLED,
                enabled
            );
            auto res = con.Query(query);
            REQUIRE(res->HasError() == false);
            CHECK(GetExtensionParams(db).cache_enabled == (enabled == "true"));
        }
    }

    for(uint64_t size : {0, 1, 1024}) {
        SECTION("Set Cache Size (LOCAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET %s = %d;",
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_SIZE,
                size
            );
            auto res = con.Query(query);
            EnsureResultError(*res);
        }

        SECTION("Set Cache Size (GLOBAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET GLOBAL %s = %d;",
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_SIZE,
                size
            );
            auto res = con.Query(query);
            REQUIRE(res->HasError() == false);
            CHECK(GetExtensionParams(db).max_cache_size == size);
        }
    }

    for(duckdb::string name : {"test_cache_0", "test_cache_1", "test_cache_2"}) {
        auto path = name + "_" + std::to_string(std::time(nullptr)) + ".bin";
        SECTION("Set Cache Path (LOCAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_PATH,
                path
            );
            auto res = con.Query(query);
            EnsureResultError(*res);
        }

        SECTION("Set Cache Path (GLOBAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET GLOBAL %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_CACHE_PATH,
                path
            );
            auto res = con.Query(query);
            REQUIRE(res->HasError() == false);
            CHECK(GetExtensionParams(db).cache_path == path);
        }

        RemoveLocalFile(path);
    }

    for(duckdb::string mutable_val : {"true", "false"}) {
        SECTION("Set Data Mutable (SESSION)") {
            auto query = duckdb::StringUtil::Format(
                "SET %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE,
                mutable_val
            );
            auto res = con.Query(query);
            REQUIRE(res->HasError() == false);
            CHECK(GetExtensionParams(*con.context).data_mutable == (mutable_val == "true"));
        }

        SECTION("Set Data Mutable (GLOBAL)") {
            auto query = duckdb::StringUtil::Format(
                "SET GLOBAL %s = '%s';", 
                ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE,
                mutable_val
            );
            auto res = con.Query(query);
            REQUIRE(res->HasError() == false);
            CHECK(GetExtensionParams(db).data_mutable == (mutable_val == "true"));
        }
    }
}

TEST_CASE_METHOD(WithDuckDB, "Change cache paths while cache is used", "[cachefs]")
{
    const duckdb::string FILENAME = "cachefs://test/testdata/read_test.txt";

    const duckdb::string INITIAL_PATH = "/tmp/cache_0.bin";
    RemoveLocalFile(INITIAL_PATH);

    const duckdb::string NEW_PATH = "/tmp/cache_1.bin";
    RemoveLocalFile(NEW_PATH);


    auto& db = GetDBInstance();
    db.config.SetOptionByName(
        ExtensionParams::PARAM_NAME_CACHEFS_CACHE_ENABLED,
        duckdb::Value::BOOLEAN(true)
    );
    REQUIRE(GetExtensionParams(GetDBInstance()).cache_enabled == true);
    db.config.SetOptionByName(
        ExtensionParams::PARAM_NAME_CACHEFS_CACHE_PATH,
        duckdb::Value(INITIAL_PATH)
    );
    REQUIRE(GetExtensionParams(GetDBInstance()).cache_path == INITIAL_PATH);

    auto con = duckdb::Connection{GetDBInstance()};
    auto& context = *con.context;
    auto& context_fs = duckdb::FileSystem::GetFileSystem(context);
    auto query = duckdb::StringUtil::Format(
        "SET GLOBAL %s = '%s';",
        ExtensionParams::PARAM_NAME_CACHEFS_CACHE_PATH,
        NEW_PATH
    );

    SECTION("Has open handles")
    {
        auto open_handle = context_fs.OpenFile(FILENAME, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        auto res = con.Query(query);
        REQUIRE(res->HasError());
        CHECK(res->GetErrorObject().Type() == duckdb::ExceptionType::IO);
        CHECK(res->GetErrorObject().RawMessage() == "Query cache is in use, please wait for the running queries to finish and try again.");
        REQUIRE(GetExtensionParams(*con.context).cache_path == INITIAL_PATH);
    }
    SECTION("Has no open handles")
    {
        auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        CHECK(GetExtensionParams(*con.context).cache_path == NEW_PATH);
    }
    SECTION("Has no open handles, but has closed ones")
    {
        auto closed_handle = context_fs.OpenFile(FILENAME, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        closed_handle->Close();
        auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        CHECK(GetExtensionParams(*con.context).cache_path == NEW_PATH);
    }

    RemoveLocalFile(INITIAL_PATH);
    RemoveLocalFile(NEW_PATH);
}

TEST_CASE_METHOD(WithDuckDB, "Data mutable parameter scope behavior", "[cache_params]") {
    auto& db = GetDBInstance();
    
    SECTION("Global setting affects all connections") {
        // Set globally
        db.config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE, duckdb::Value::BOOLEAN(false));
        
        // Check that new connections inherit the global setting
        auto con1 = duckdb::Connection{db};
        auto con2 = duckdb::Connection{db};
        
        CHECK(GetExtensionParams(db).data_mutable == false);
        CHECK(GetExtensionParams(*con1.context).data_mutable == false);
        CHECK(GetExtensionParams(*con2.context).data_mutable == false);
        
        // Change global setting
        db.config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE, duckdb::Value::BOOLEAN(true));
        
        // New connections should get the new global value
        auto con3 = duckdb::Connection{db};
        CHECK(GetExtensionParams(*con3.context).data_mutable == true);
    }
    
    SECTION("Session settings are independent") {
        // Set a global default
        db.config.SetOptionByName(ExtensionParams::PARAM_NAME_CACHEFS_DATA_MUTABLE, duckdb::Value::BOOLEAN(true));
        
        auto con1 = duckdb::Connection{db};
        auto con2 = duckdb::Connection{db};
        
        // Both should start with global value
        CHECK(GetExtensionParams(*con1.context).data_mutable == true);
        CHECK(GetExtensionParams(*con2.context).data_mutable == true);
        
        // Set session-specific value for con1
        auto res1 = con1.Query("SET cachefs_data_mutable = false;");
        REQUIRE_FALSE(res1->HasError());
        
        // con1 should have session value, con2 should still have global value
        CHECK(GetExtensionParams(*con1.context).data_mutable == false);
        CHECK(GetExtensionParams(*con2.context).data_mutable == true);
        CHECK(GetExtensionParams(db).data_mutable == true); // Global unchanged
        
        // Set different session value for con2
        auto res2 = con2.Query("SET cachefs_data_mutable = false;");
        REQUIRE_FALSE(res2->HasError());
        
        // Now con1 can change its session value independently
        auto res3 = con1.Query("SET cachefs_data_mutable = true;");
        REQUIRE_FALSE(res3->HasError());
        
        CHECK(GetExtensionParams(*con1.context).data_mutable == true);
        CHECK(GetExtensionParams(*con2.context).data_mutable == false);
        CHECK(GetExtensionParams(db).data_mutable == true); // Global still unchanged
    }
    
    SECTION("SET GLOBAL affects existing and new connections") {
        auto con1 = duckdb::Connection{db};
        
        // Set session value
        auto res1 = con1.Query("SET cachefs_data_mutable = false;");
        REQUIRE_FALSE(res1->HasError());
        CHECK(GetExtensionParams(*con1.context).data_mutable == false);
        
        // Set global value via SQL
        auto res2 = con1.Query("SET GLOBAL cachefs_data_mutable = true;");
        REQUIRE_FALSE(res2->HasError());
        
        // Global should be updated, but existing session value should remain
        CHECK(GetExtensionParams(db).data_mutable == true);
        CHECK(GetExtensionParams(*con1.context).data_mutable == false); // Session value preserved
        
        // New connection should get the new global value
        auto con2 = duckdb::Connection{db};
        CHECK(GetExtensionParams(*con2.context).data_mutable == true);
    }
}
