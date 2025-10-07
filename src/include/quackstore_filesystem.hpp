#pragma once

#include <duckdb.hpp>
#include "cache.hpp"

namespace quackstore {

class QuackstoreFileSystem : public duckdb::FileSystem {
public:
    static constexpr const char* FILESYSTEM_NAME = "QuackstoreFileSystem";
    static constexpr const char* SCHEMA_PREFIX = "quackstore://";

    QuackstoreFileSystem(Cache& cache);

public:
    // FileSystem methods
    duckdb::unique_ptr<duckdb::FileHandle> OpenFile(const duckdb::string &path, duckdb::FileOpenFlags flags,
                                    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
    void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
    int64_t Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes) override;
    int64_t GetFileSize(duckdb::FileHandle &handle) override;
    duckdb::string GetName() const override { return FILESYSTEM_NAME; }
    bool CanHandleFile(const duckdb::string &path) override;
    duckdb::vector<duckdb::OpenFileInfo> Glob(const duckdb::string &path, duckdb::FileOpener *opener = nullptr) override;
    void Seek(duckdb::FileHandle &handle, idx_t location) override;
    bool OnDiskFile(duckdb::FileHandle &handle) override { return false; }
    idx_t SeekPosition(duckdb::FileHandle &handle) override;
    bool CanSeek() override { return true; }
    duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle &handle) override;
    bool IsManuallySet() override { return true; }

private:
    Cache& cache;
};

}  // namespace quackstore
