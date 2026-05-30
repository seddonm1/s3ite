# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-05-30

### Added

- Add a `scripts/setup-s3-tests.sh` helper to install `uv`, configure Ceph `s3-tests`, and run curated or custom conformance tests against a local s3ite server.
- Add `Config::readers` with a default of `4` and wire it to the existing `--readers` CLI option, separating SQLite reader threads from HTTP request concurrency.
- Add unversioned `ListObjectVersions` support that reports current objects as `VersionId = null`.
- Add minimal object ACL reads for conformance tests, returning the stable local owner with full control.
- Add `AbortMultipartUpload` support.

### Changed

- Upgrade dependencies, including `s3s` to `0.13`, `rusqlite` to `0.40`, Tokio/Hyper/Tower updates, and the AWS test stack pinned to compatible versions.
- Replace deprecated `serde_yaml` with the maintained `yaml_serde` fork for YAML configuration parsing.
- Upgrade the crate to Rust edition 2024.
- Change the default SQLite cache size from `67_108_864` KiB to `65_536` KiB.
- Rework SQLite connection management so read threads are configured independently, database queues fail fast when full or closed, and shutdown joins worker threads instead of busy-waiting.
- Replace process-global listing continuation snapshots with keyset pagination over ordered keys.
- Reduce the direct dependency surface by removing unused dependencies, feature-gating binary-only crates, and replacing `crossbeam-channel` worker queues with `std::sync::mpsc::sync_channel`.
- Ignore local reference clones and local `s3-tests` runtime artifacts.

### Fixed

- Fix ranged `GetObject` responses to use normalized inclusive/exclusive byte bounds consistently, including suffix ranges, zero-byte objects, `Content-Range`, and `Accept-Ranges`.
- Enforce read-only buckets for `DeleteObject` and `UploadPart`.
- Fix bucket deletion to reject non-empty buckets before closing/removing the SQLite database.
- Make object deletes and multi-object deletes match S3 idempotency expectations, including missing keys, quiet deletes, request size limits, and trailing-slash object keys.
- Fix `ListObjects`, `ListObjectsV2`, and `ListObjectVersions` prefix, delimiter, common-prefix, marker, continuation-token, `KeyCount`, owner, and URL-encoding semantics, including literal `%` and `_` key characters.
- Preserve and return object `ETag` from `HeadObject`.
- Persist and return object HTTP metadata such as `Content-Type`, `Cache-Control`, and `Expires`, including copy and multipart completion paths.
- Fix `CopyObject` metadata replacement, self-copy behavior, and metadata retention.
- Fix multipart completion validation for missing, mismatched, out-of-order, and too-small parts.
- Compute multipart ETags as `md5(concat(part-md5s))-N` and build completed objects with preallocated buffers.
- Allow resent multipart parts to replace the previous stored part.
- Make repeated multipart completion idempotently return the completed multipart ETag.
- Remove stray stdout from bucket creation.

## [0.6.0] - 2025-06-13

- Bump package versions in `Cargo.toml`.

## [0.5.0] - 2024-03-02

- Replace deadpool-sqlite with a connector based on tokio-rusqlite.
- Rework error system.

## [0.3.3] - 2023-10-08

## Changed

- Bump package versions in `Cargo.toml`.

## [0.3.2] - 2023-07-20

## Changed

- Release `ghcr` docker image.

## [0.3.1] - 2023-07-18

## Fixed

- Bump version in `Cargo.toml`.

## [0.3.0] - 2023-07-16

## Changed

- Split the core data table into `data` and `metadata`.
- Add a simple garbage collection task to clean up any redundant state. E.g. from a client cancelling an in-progress `list_objects` task.
- Remove SQLite `VACUUM` command on startup due to potentially large memory usage.

### Fixed

- Snapshot database `metadata` state on `list_objects` so that deletes will remove all objects.

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
