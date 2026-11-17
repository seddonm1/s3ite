# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Stored MD5, SHA512, XXHASH3, XXHASH64, and XXHASH128 checksums for object writes, reads, copies, and composite multipart uploads.

- Object writes and multipart uploads preserve `Content-Disposition` and `Content-Language` for GET, HEAD, and COPY.

### Fixed

- Object writes reject an empty `x-amz-server-side-encryption` header with `400 InvalidArgument`.
- Copying an object onto itself solely to replace tags returns `400 InvalidRequest`.

- Bucket encryption configurations containing multiple rules return `400 MalformedXML`. Reading an unconfigured bucket’s encryption settings returns `404 ServerSideEncryptionConfigurationNotFoundError`.

- Bucket CORS rules now reject cross-origin requests with a disallowed origin or HTTP method before the operation runs, including requests made with presigned URLs.

- CopyObject now checks destination If-Match and If-None-Match conditions before writing. A rejected copy preserves the destination body and version history.

### Changed

- Updated Rust dependencies and delegated SigV2 authentication to `s3s` 0.17. Clients sending RFC 2822 dates in `x-amz-date` should use the compact UTC format or send an RFC 1123 `Date` header. Bucket creation signatures must match the path on the wire.

- Docker images now build a fully static musl binary for a `scratch` runtime image, using `tikv-jemallocator`, fat LTO, and `cargo-multivers` 0.13.0.
- AArch64 musl C builds pass `-mno-outline-atomics` so bundled SQLite and jemalloc avoid GCC outline atomics that depend on `getauxval` support unavailable in this musl build path.

## [0.7.0] - 2026-05-30

### Changed

- **Breaking change: data from older `s3ite` versions must be exported and reimported.** Treat `0.7.0` as a rewrite of `s3ite`, not as a small incremental release. The server, storage layer, schema, configuration model, test suite, logging coverage, and release build path were all substantially rebuilt.
- `s3ite` is now a binary-only crate. The library target and binary feature gate were removed, and runtime/server dependencies are always enabled.
- The supported S3 surface is now a focused owner-only object store: bucket and object CRUD, list v1/v2/version listings, copy, multipart, checksums, object attributes, conditionals, ranges, raw auth and presign compatibility, and owner-equivalent ACL compatibility.
- Unsupported surfaces remain intentionally out of scope, including IAM, STS, SNS, S3 Select, S3 Control, torrent, public or anonymous access, public ACL grants, bucket policies, POST form uploads, per-bucket CORS APIs, lifecycle/restore, SSE/encryption, tagging, object lock, S3 logging, object ownership controls, public access block, and multi-owner behavior.
- CI now enforces Rust formatting, clippy warnings, and `cargo llvm-cov` line coverage thresholds for both overall and per-file source coverage.
- Configuration was rebuilt around a binary CLI plus YAML service and bucket overrides, including reader/blob-reader pools, maintenance and multipart cleanup TTLs, read-only mode, SQLite pragma overrides, virtual-hosted-style domains, and disabled/permissive/custom CORS modes.
- SQLite connections now apply connection-local reader and writer pragmas, disable trusted schemas, use a busy timeout, cap retained journal/WAL size, and run modern `PRAGMA optimize` maintenance.
- Docker builds now use `cargo-multivers` to produce the release binary.
- Existing bucket databases from older schema versions are not migrated in place; unsupported schemas are rejected at startup.

## [0.6.0] - 2025-06-13

### Changed

- Bump package versions in `Cargo.toml`.

## [0.5.0] - 2024-03-02

### Changed

- Replace `deadpool-sqlite` with a connector based on `tokio-rusqlite`.
- Rework error system.

## [0.3.3] - 2023-10-08

### Changed

- Bump package versions in `Cargo.toml`.

## [0.3.2] - 2023-07-20

### Changed

- Release `ghcr` Docker image.

## [0.3.1] - 2023-07-18

### Fixed

- Bump version in `Cargo.toml`.

## [0.3.0] - 2023-07-16

### Changed

- Split the core data table into `data` and `metadata`.
- Add a simple garbage collection task to clean up redundant state, such as a client cancelling an in-progress `list_objects` task.
- Remove `SQLite` `VACUUM` command on startup due to potentially large memory usage.

### Fixed

- Snapshot database `metadata` state on `list_objects` so that deletes will remove all objects.

## [0.2.0] - 2023-07-05

### Fixed

- Correct verification of `Content-MD5` when provided by the client.

### Added

- Delete leftover `-wal` or `-shm` database artifacts when removing a bucket.
- Add `config` YAML format support for complex configurations.

### Changed

- Create `SQLite` tables without `STRICT` mode enabled to allow greater backward compatibility.
- Issue `SQLite` `VACUUM` command on startup.

## [0.1.0] - 2023-06-27

### Added

- An initial release proof-of-concept.
- Works for the major get/put/delete/list operations and supports Cross-Origin Resource Sharing (`CORS`).
- Supports presigned URLs.
