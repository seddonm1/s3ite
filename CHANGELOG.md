# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-05-30

### Added

- Add a `scripts/run-s3-tests.sh` helper for local Ceph `s3-tests` runs with optional pytest arguments, including temp-workspace checkout, `uv` setup, config generation, and local `s3ite` lifecycle management.
- Add `Config::readers` with a default of `4` and wire it to the existing `--readers` CLI option, separating `SQLite` metadata reader threads from HTTP request concurrency.
- Add `Config::blob_readers` with a default of `16` and wire it to `--blob-readers`, giving long-lived `GetObject` BLOB streams their own reader pool.
- Add `Config::blob_read_timeout_seconds` with a default of `120` and wire it to `--blob-read-timeout-seconds`, releasing BLOB readers blocked by unconsumed responses.
- Add `maintenance_interval_seconds` configuration for periodic database maintenance, with `0` disabling the background task.
- Add `maintenance_vacuum_pages` configuration for `PRAGMA incremental_vacuum(N)`, with `0` skipping incremental vacuum during maintenance.
- Add default-on multipart cleanup TTL configuration for inactive incomplete uploads and completed-upload idempotency records, defaulting both to `86400` seconds with `0` disabling each cleanup.
- Add unversioned `ListObjectVersions` support that reports current objects as `VersionId = null`.
- Add minimal object ACL reads for conformance tests, returning the stable local owner with full control.
- Add `AbortMultipartUpload` support.
- Add multipart upload listing, multipart copy, completed-object part reads, and completed part boundary tracking.
- Add `GetObjectAttributes` support for `ETag`, object size, storage class, multipart object parts, and checksum attributes.
- Add checksum validation, persistence, and response fields for single-part and multipart uploads, including `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, and `SHA256`.

### Changed

- Upgrade dependencies, including `s3s` to `0.13`, `rusqlite` to `0.40`, `tokio`/`hyper`/`tower` updates, and the AWS test stack pinned to compatible versions.
- Replace deprecated `serde_yaml` with the maintained `yaml_serde` fork for YAML configuration parsing.
- Upgrade the crate to Rust edition `2024`.
- Change the default `SQLite` cache size from `67_108_864` KiB to `65_536` KiB.
- Rework `SQLite` connection management so read threads are configured independently, database queues fail fast when full or closed, and shutdown joins worker threads instead of busy-waiting.
- Return `SqliteResult`/`SqliteError` from `SQLite` storage operations, remove the crate-wide `S3ite` error module, and map storage outcomes to S3 errors at the service boundary.
- Replace process-global listing continuation snapshots with keyset pagination over ordered keys.
- Reduce the direct dependency surface by removing unused dependencies, feature-gating binary-only crates, and replacing `crossbeam-channel` worker queues with `std::sync::mpsc::sync_channel`.
- Ignore local reference clones and local `s3-tests` runtime artifacts.
- Run the local `s3-tests` helper against a release `s3ite` build and exclude `fails_on_rgw` tests by default.
- Generate conformance-test server config with permissive `CORS` disabled so presigned request validation reaches the S3 auth layer.
- Split object metadata from object bodies in the versioned `SQLite` schema and stream `GetObject` response bodies from incremental `SQLite` BLOB reads.

### Fixed

- Fix ranged `GetObject` responses to use normalized inclusive/exclusive byte bounds consistently, including suffix ranges, zero-byte objects, `Content-Range`, and `Accept-Ranges`.
- Enforce read-only buckets for `DeleteObject` and `UploadPart`.
- Fix bucket deletion to reject non-empty buckets before closing/removing the `SQLite` database.
- Make object deletes and multi-object deletes match S3 idempotency expectations, including missing keys, quiet deletes, request size limits, and trailing-slash object keys.
- Fix `ListObjects`, `ListObjectsV2`, and `ListObjectVersions` prefix, delimiter, common-prefix, marker, continuation-token, `KeyCount`, owner, and URL-encoding semantics, including literal `%` and `_` key characters.
- Preserve and return object `ETag` from `HeadObject`.
- Persist and return object HTTP metadata such as `Content-Type`, `Cache-Control`, and `Expires`, including copy and multipart completion paths.
- Fix `CopyObject` metadata replacement, self-copy behavior, and metadata retention.
- Apply conditional request headers for `GetObject`, `HeadObject`, `PutObject`, `CopyObject`, `DeleteObject`, multi-object delete, and multipart completion.
- Fix multipart completion validation for missing, mismatched, out-of-order, and too-small parts.
- Compute multipart ETags as `md5(concat(part-md5s))-N` and build completed objects with preallocated buffers.
- Allow resent multipart parts to replace the previous stored part.
- Make repeated multipart completion idempotently return the completed multipart ETag.
- Refresh incomplete multipart upload activity when parts are uploaded, so cleanup is based on inactivity rather than creation time.
- Complete `ListParts` pagination and `ETag` behavior, and normalize invalid multipart upload IDs to `NoSuchUpload`.
- Return `InvalidDigest` for malformed `Content-MD5` values while preserving `BadDigest` for valid digests that do not match the request body.
- Reject DNS-incompatible bucket names such as `foo-.bar`, `foo.-bar`, and IPv4-address-shaped names.
- Enforce S3 presigned URL expiry bounds and return expected errors for invalid expiry values and `OPTIONS` probes.
- Remove stray stdout from bucket creation.

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
