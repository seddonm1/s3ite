# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

**Upgrading from v0.6.0 requires an HTTP copy into a new data directory.**
The upcoming 0.7.0 server checks each bucket's SQLite schema version at startup
and requires version `70`. The migration script copies current objects and
verifies their contents and metadata. [Migration instructions](README.md#migrate-from-v060-over-http)

### Added

- Added an opt-in HTTP listener for `/livez` and `/readyz` probes during startup, service, and graceful shutdown.
- Added bucket versioning, object version listings, multipart upload listing and abort, upload-part copy, object attributes, and conditional writes.
- Added browser POST uploads, per-bucket CORS, bucket and object tagging, lifecycle expiration and incomplete-upload cleanup, SSE-S3 encryption, and Object Lock retention and legal holds.
- Added SSE-KMS for object writes, reads, multipart, copies, and bucket encryption defaults using an AWS KMS-compatible service.
- Added stored MD5, SHA512, XXHASH3, XXHASH64, and XXHASH128 checksums for object writes, reads, copies, and composite multipart uploads.
- Object writes and multipart uploads preserve `Content-Disposition` and `Content-Language` for GET, HEAD, and COPY.
- Added runtime-configurable OTLP/HTTP export for distributed traces, audit logs, and operational metrics. Incoming requests propagate W3C trace context, and bulk deletes emit individual audit outcomes.

### Changed

- s3ite is now a binary-only crate. The library target and binary feature gate were removed.
- The CLI and YAML configuration set database access for the whole service, including read-only mode, SQLite pragmas, reader pools, and maintenance settings. YAML bucket overrides can change read-only mode and SQLite pragmas per bucket.
- SQLite connections apply reader and writer pragmas, disable trusted schemas, use a busy timeout, limit retained journal size, and run periodic `PRAGMA optimize` maintenance.
- Updated Rust dependencies to `s3s` 0.17. SigV2 requests are rejected with `AccessDenied`, as AWS does for buckets created since June 2020; clients must sign with SigV4.
- Docker images use a fully static musl binary in a `scratch` runtime image, built with `tikv-jemallocator`, fat LTO, and `cargo-multivers`. AArch64 builds disable outline atomics for bundled C code.
- CI checks formatting, Clippy warnings, and 95% line coverage both overall and per source file.

### Fixed

- Startup rejects a YAML configuration containing only one of `access_key` and `secret_key`.
- Object writes reject an empty `x-amz-server-side-encryption` header with `400 InvalidArgument`.
- A tag-only self-copy returns `400 InvalidRequest`.
- Bucket encryption configurations containing multiple rules return `400 MalformedXML`. Reading an unconfigured bucket’s encryption settings returns `404 ServerSideEncryptionConfigurationNotFoundError`.
- Bucket CORS rules reject disallowed origins and HTTP methods before the operation runs, including for presigned requests.
- CopyObject checks destination `If-Match` and `If-None-Match` conditions before writing. A rejected copy preserves the destination body and version history.

## [0.6.0] - 2025-06-13

### Changed

- Bump package versions in `Cargo.toml`.

## [0.5.0] - 2024-03-02

### Changed

- Replace `deadpool-sqlite` with a connector based on `tokio-rusqlite`.
- Rework error system.

## [0.4.0] - 2023-11-18

### Changed

- Updated crate dependencies and the development container to Rust 1.74.0.

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
