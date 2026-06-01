# s3ite

This is an experimental binding using the [s3s](https://crates.io/crates/s3s) crate backed by [SQLite](https://www.sqlite.org).

**HIGHLY EXPERIMENTAL** do not use in production. The project now has broader local and Ceph `s3-tests` coverage, but the API surface is still incomplete and data loss cannot be guaranteed.

## Why

This crate was built to test the feasibility of using SQLite as an alternative to formats like [TFRecord](https://www.tensorflow.org/tutorials/load_data/tfrecord) for storing a large number of machine learning training data with an accessible API. Although `TFRecord` and SQLite share a similar few-large-storage-files architecture - making backups and data movement efficient/easy - having both a SQLite interface and [Amazon S3](https://aws.amazon.com/s3/)/`http` API makes access to the contained data easier with tooling that likely already exists on your machine.

This concept is backed by benchmarks from SQLite showing that it can be [faster than filesystems](https://www.sqlite.org/fasterthanfs.html) for certain data access patterns.

## Architecture

Each `bucket` is saved to a separate `.sqlite3` database named after the `bucket` name. The [smithy](https://github.com/awslabs/smithy) generated bindings for `s3` are then mapped to the correct SQL calls against a very simple schema that is designed to be human accessible.

`Content-MD5` verification (if available) and SQLite [database transactions](https://sqlite.org/transactional.html) are used to prevent data loss or partial updates.

### Data

Object metadata is stored separately from object bodies. `object_version` keeps the small, frequently scanned state used by `HEAD`, `LIST`, conditionals, and versioning. `object_body` stores the BLOB payload by the same `sequence` rowid. Keeping payload bytes out of the metadata table lets SQLite cache listing and metadata pages more effectively. This idea was inspired by [BadgerDB](https://github.com/outcaste-io/badger) and the WiscKey paper.

```sql
CREATE TABLE object_version (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL,
    is_delete_marker INTEGER NOT NULL,
    size INTEGER NOT NULL,
    metadata TEXT,
    content_type TEXT,
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    last_modified TEXT NOT NULL,
    md5 TEXT,
    checksum_algorithm INTEGER,
    checksum_type INTEGER,
    checksum_value TEXT,
    UNIQUE(key, version_id)
) STRICT;

CREATE TABLE object_body (
    sequence INTEGER PRIMARY KEY,
    body BLOB NOT NULL,
    FOREIGN KEY (sequence) REFERENCES object_version (sequence) ON DELETE CASCADE
) STRICT;
```

Bucket databases are checked at startup against the supported schema version. Unsupported older schemas fail to open rather than being migrated in place.

### Multipart Uploads

For multipart uploads, s3ite uses temporary upload tables plus a completion ledger for idempotent completion responses:

```sql
CREATE TABLE IF NOT EXISTS multipart_upload (
    upload_id               BLOB NOT NULL PRIMARY KEY,
    bucket                  TEXT NOT NULL,
    key                     TEXT NOT NULL,
    last_modified           TEXT NOT NULL,
    access_key              TEXT,
    metadata                TEXT,
    content_type            TEXT,
    cache_control           TEXT,
    expires                 TEXT,
    UNIQUE(upload_id, bucket, key)
);

CREATE TABLE IF NOT EXISTS multipart_upload_part (
    upload_id               BLOB NOT NULL,
    last_modified           TEXT NOT NULL,
    part_number             INTEGER NOT NULL,
    value                   BLOB NOT NULL,
    size                    INTEGER NOT NULL,
    md5                     TEXT,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS completed_multipart_upload (
    upload_id               BLOB NOT NULL PRIMARY KEY,
    bucket                  TEXT NOT NULL,
    key                     TEXT NOT NULL,
    access_key              TEXT,
    md5                     TEXT NOT NULL,
    last_modified           TEXT NOT NULL,
    UNIQUE(upload_id, bucket, key)
);
```

## Configuration

`s3ite` provides configuration options at the `service` level (i.e. the global level that applies to all buckets or controls API behavior) or at the `bucket` level for changing specific bucket behavior. These can be set with a YAML configuration file or the command-line interface.

The YAML configuration (provided via the `--config` argument) differs from the command-line options in that it allows parameters to be specified at the `service` and `bucket` levels, whereas the command-line interface provides only `service` level configuration.

The defaults are intended to be enough for normal local execution; set options
only when a deployment needs to override them. The available options are:

- `root`: The base path where the `.sqlite3` files will be created.
- `host`: The IP address to listen on for this service.
- `port`: The port to listen on for this service.
- `access_key`: The access key ID that is used to authenticate for this service.
- `secret_key`: The secret access key that is used to authenticate for this service.
- `concurrency_limit`: Enforces a limit on the concurrent number of HTTP requests the service can handle. This can be tuned depending on infrastructure as SSD/HDD will handle resource contention very differently.
- `readers`: Controls the number of short metadata/query `SQLite` reader connections. Defaults to `4`.
- `blob_readers`: Controls the number of long-lived `GetObject` BLOB stream reader connections. Defaults to `16`.
- `blob_read_timeout_seconds`: Seconds a BLOB reader may stay blocked by an unconsumed response before releasing its SQLite handle. Defaults to `120`; set to `0` to disable.
- `maintenance_interval_seconds`: Interval for background database maintenance. Defaults to `60`; set to `0` to disable periodic maintenance.
- `maintenance_vacuum_pages`: Number of free SQLite pages to reclaim with `PRAGMA incremental_vacuum(N)` during each maintenance pass. Defaults to `100`; set to `0` to skip incremental vacuum.
- `multipart_upload_ttl_seconds`: TTL for inactive incomplete multipart uploads. Defaults to `86400`; set to `0` to disable background cleanup.
- `completed_multipart_upload_ttl_seconds`: TTL for completed multipart idempotency records. Defaults to `86400`; set to `0` to keep records until explicitly removed.
- `permissive_cors`: Allow permissive Cross-Origin Resource Sharing (CORS) requests. This can be enabled to allow users to access this service from a web service running on a different host.
- `domain_name`: The domain to use to allow parsing virtual-hosted-style requests.
- `read_only`: Prevent mutations to any of the databases connected to this service.
- `journal_mode`: Controls the default SQLite [journal_mode](https://www.sqlite.org/pragma.html#pragma_journal_mode) pragma.
- `synchronous`: Controls the default SQLite [synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous) pragma.
- `temp_store`: Controls the default SQLite [temp_store](https://www.sqlite.org/pragma.html#pragma_temp_store) pragma.
- `cache_size`: Controls the default SQLite [cache_size](https://www.sqlite.org/pragma.html#pragma_cache_size) pragma in KiB. Defaults to `65536`.

This structure is hierarchical where:

### Command-Line Service Configuration

Command-line arguments take precedence over any `service` level configuration, like below where the default `root` value `.` is overridden with `/data`.

```bash
s3ite --root /data
```

### Bucket Level Configuration

If set, `bucket` level configurations will take precedence over the `service` level configurations.

This design allows setting specific `bucket` level configurations like below where the `mybucket` bucket will be set to `read-only` on startup and all other buckets will be writable. An error will be raised if any specified bucket does not exist (in this case, an error is raised if `mybucket.sqlite3` is not found and accessible).

```yaml
root: /data
access_key: AKIAIOSFODNN7EXAMPLE
secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
buckets:
  mybucket:
    read_only: true
    sqlite:
      cache_size: 131072
```

If `access_key` and `secret_key` are omitted, authentication is disabled.

### Auth Model

s3ite uses a Level 1 owner-only auth model. When `access_key` and `secret_key` are configured, valid signed requests and valid presigned URLs act as the single owner identity, and unsigned requests are denied. When those keys are omitted, auth is disabled and every request behaves as that owner.

ACLs are not a permission system in s3ite. `GetBucketAcl` and `GetObjectAcl` return a synthetic owner `FULL_CONTROL` ACL for compatibility, and owner-equivalent ACL writes such as `private` are accepted as no-ops. Public ACLs, explicit grant headers, ACL XML bodies, IAM, bucket policies, object ownership controls, and anonymous public access are unsupported. Use presigned URLs for temporary object sharing.

## Docker

```bash
docker run --rm \
-e RUST_LOG=info \
-v $(pwd)/test:/data \
-p 8014:8014 \
s3ite:latest \
./s3ite \
--root /data \
--host 0.0.0.0 \
--port 8014 \
--concurrency-limit 16 \
--readers 4 \
--blob-readers 16 \
--blob-read-timeout-seconds 120 \
--access-key AKIAIOSFODNN7EXAMPLE \
--secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```


## Build

This code can be used as a library or a standalone binary. It uses Rust edition 2024, so use a compatible Rust toolchain. To build the binary:

```bash
cargo build --release
```

## Install

```bash
cargo install --path .
```

## Run

```bash
s3ite --root . --host 0.0.0.0 --port 8014 --access-key AKIAIOSFODNN7EXAMPLE --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Conformance Tests

The repository includes a helper for running a local Ceph `s3-tests` environment. Run the full functional suite with:

```bash
scripts/run-s3-tests.sh
```

The helper clones Ceph `s3-tests` if needed, installs `uv` if needed, creates the Python environment, builds and starts a local `s3ite` server, runs pytest, and stops the server before exiting. By default, it excludes tests marked `fails_on_rgw`; pass an explicit pytest marker expression with `-m` to override that filter. The checkout, virtual environment, Cargo target directory, generated configs, data root, and logs live under `${TMPDIR:-/tmp}/s3ite-s3-tests`.

To run a specific test, pass pytest arguments directly:

```bash
scripts/run-s3-tests.sh s3tests/functional/test_s3.py::test_object_write_read_update_read_delete
```

The current Level 1 auth model intentionally excludes ACL, public/anonymous access, ownership-control, IAM, and policy surfaces. To focus the Ceph suite on the supported object core, add a pytest name filter:

```bash
scripts/run-s3-tests.sh -q --tb=short \
  -k 'not acl and not grant and not public and not anonymous and not ownership and not policy and not iam'
```
