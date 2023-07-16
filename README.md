# s3ite

This is an experimental binding using the [s3s](https://crates.io/crates/s3s) crate backed by [SQLite](https://www.sqlite.org).

**HIGHLY EXPERIMENTAL** do not use in production as only basic tests have been run and data loss cannot be guaranteed.

## Why

This crate was built to test the feasiblity of using SQLite as an alternative to formats like [TFRecord](https://www.tensorflow.org/tutorials/load_data/tfrecord) for storing a large number of machine learning training data with an accessible API. Although `TFRecord` and SQLite share a similar few-large-storage-files architecture - making backups and data movement efficient/easy - having both a SQLite interface and [Amazon S3](https://aws.amazon.com/s3/)/`http` API makes access to the contained data easier with tooling that likely already exists on your machine.

This concept is backed by benchmarks from SQLite showing that it can be [faster than filesytems](https://www.sqlite.org/fasterthanfs.html) for certain data access patterns.

## Architecture

Each `bucket` is saved to a separate `.sqlite3` database named after the `bucket` name. The [smithy](https://github.com/awslabs/smithy) generated bindings for `s3` are then mapped to the correct SQL calls against a very simple schema that is designed to be human accessible.

`content-md5` verification (if available) and SQLite [database transactions](https://sqlite.org/transactional.html) are used to prevent data loss or partial updates.

### Data

The main table, `data`, is a simple key/value table with a separate `metadata` store. `metadata` is split from `data` as it was found to be more performant for large `list_objects` calls presumably due to it being able to be cached by the SQLite engine. This idea was inspired by [BadgerDB](https://github.com/outcaste-io/badger) who implemented ideas from the WISCKEY paper and saw big wins with separating values from keys.

```sql
CREATE TABLE IF NOT EXISTS data (
    key TEXT PRIMARY KEY,
    value BLOB
);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    metadata TEXT,
    last_modified TEXT NOT NULL,
    md5 TEXT,
    FOREIGN KEY (key) REFERENCES data (key) ON DELETE CASCADE
) WITHOUT ROWID;
```

### Multipart Uploads

For `multipart` uploads two temporary tables are used:

```sql
CREATE TABLE IF NOT EXISTS multipart_upload (
    upload_id               BLOB NOT NULL PRIMARY KEY,
    bucket                  TEXT NOT NULL,
    key                     TEXT NOT NULL,
    last_modified           TEXT NOT NULL,
    access_key              TEXT,
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
```

## Configuration

`s3ite` provides configuration options at the `service` level (i.e. the global level that apply to all buckets or control the API behavior) or at the `bucket` level for changing specific bucket behavior. To set them `sqlite` has two methods of configuration: a `yaml` configuration file or the command-line-interface.

The `yaml` configuration (provided via the `--config` argument) differs from the `command-line-interface` options in that it allows parameters be specified at the `service` and `bucket` levels whereas the `command-line-interface` provides only `service` level configuration.

The options are:

- `root`: The base path where the `.sqlite3` files will be created.
- `host`: The IP address to listen on for this service.
- `port`: The port to listen on for this service.
- `access_key`: The access key ID that is used to authenticate for this service.
- `secret_key`: The secret access key that is used to authenticate for this service.
- `concurrency_limit`: Enforces a limit on the concurrent number of requests the underlying service can handle. This can be tuned depending on infrastructure as SSD/HDD will handle resource contention very differently.
- `permissive_cors`: Allow permissive Cross-Origin Resource Sharing (CORS) requests. This can be enabled to allow users to access this service from a web service running on a different host.
- `domain_name`: The domain to use to allow parsing virtual-hosted-style requests.
- `read_only`: Prevent mutations to any of the databases connected to this service.
- `journal_mode`: Controls the default SQLite [journal_mode](https://www.sqlite.org/pragma.html#pragma_journal_mode) pragma.
- `synchronous`: Controls the default SQLite [synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous) pragma.
- `temp_store`: Controls the default SQLite [temp_store](https://www.sqlite.org/pragma.html#pragma_temp_store) pragma.
- `cache_size`: Controls the default SQLite [cache_size](https://www.sqlite.org/pragma.html#pragma_cache_size) pragma.

This structure is heirarchical where:

### Command-Line Service Configuration

`command-line-interface` arguments will take precedence over any `service` level configuration like below where the default `root` value `.` is overridden with `/data`.

```bash
s3ite --root /data
```

### Bucket Level Configuration

If set, `bucket` level configurations will take precedence over the `service` level configurations.

This design allows setting specific `bucket` level configurations like below where the `mybucket` bucket will be set to `read-only` on startup and all other buckets will be writable. An error will be raised if any specified bucket does not exist (in this case and error is raised if `mybucket.sqlite3` is not found and accessible).

```yaml
root: /data
host: 0.0.0.0
port: 8014
access_key: AKIAIOSFODNN7EXAMPLE
secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
permissive_cors: true
concurrency: 16
read_only: false
journal_mode: WAL
synchronous: NORMAL
temp_store: MEMORY
cache_size: 67108864
buckets:
  mybucket:
    read_only: true
    sqlite:
      cache_size: 134217728
```

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
--access-key AKIAIOSFODNN7EXAMPLE \
--secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```


## Build

This code can be used as a library or a standalone binary. To build the binary:

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

