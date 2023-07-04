# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2023-07-05

### Fixed

- Correct verification of `Content-MD5` if provided by client.

### Added

- Delete leftover `-wal` or `-shm` database artifacts when removing a bucket.
- `config` YAML format configurations to supply complex configurations.

### Changed

- SQLite tables are now created without `STRICT` mode enabled. This is to allow greater backward compatibility.
- Issue SQLite `VACUUM` command on startup.

## [0.1.0] - 2023-06-27

### Added

- An initial release proof-of-concept.
- Works for the major get/put/delete/list operations and supports Cross-Origin Resource Sharing (CORS).
- Supports presigned URLs.