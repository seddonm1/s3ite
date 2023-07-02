# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Delete leftover `-wal` or `-shm` database artifacts when removing a bucket.
- `config` YAML format configurations to supply complex configurations.
- `concurrency_limit` to limit on the concurrent number of requests the underlying service can handle. Default `16`.
- `read_only` to control whether this service/bucket is read-only. This is enforced at both the S3 API and SQLite (`query_only` pragma) level. Default: `false`.
- `journal_mode` to control the SQLite `journal_mode` pragma. Default: `WAL`.
- `synchronous` to control the SQLite `synchronous` pragma. Default: `NORMAL`.
- `temp_store` to control the SQLite `temp_store` pragma. Default: `MEMORY`.
- `cache_size` to control the SQLite `cache_size` pragma. Default: `67108864`.

### Changed

- SQLite tables are now created without `STRICT` mode enabled. This is to allow greater backward compatibility.

## [0.1.0] - 2023-06-27

### Added

- An initial release proof-of-concept.
- Works for the major get/put/delete/list operations and supports Cross-Origin Resource Sharing (CORS).
- Supports presigned URLs.