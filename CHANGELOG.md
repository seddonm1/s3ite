# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Delete leftover `-wal` or `-shm` database artifacts when removing a bucket.
- `concurrency_limit` to limit on the concurrent number of requests the underlying service can handle. Default `16`.
- `sqlite_journal_mode` to control the SQLite `journal_mode` pragma. Default: `WAL`.
- `sqlite_synchronous` to control the SQLite `synchronous` pragma. Default: `NORMAL`.
- `sqlite_temp_store` to control the SQLite `temp_store` pragma. Default: `MEMORY`.
- `sqlite_cache_size` to control the SQLite `cache_size` pragma. Default: `67108864`.
- `sqlite_query_only` to control the SQLite `query_only` pragma and prevent mutations to the database if set. Default: `false`.

### Changed

- SQLite tables are now created without `STRICT` mode enabled. This is to allow greater backward compatibility.

## [0.1.0] - 2023-06-27

### Added

- An initial release proof-of-concept.
- Works for the major get/put/delete/list operations and supports Cross-Origin Resource Sharing (CORS).
- Supports presigned URLs.