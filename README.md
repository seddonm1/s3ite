# s3ite

This is an experimental binding using the [s3s](https://crates.io/crates/s3s) crate backed by [SQLite](https://www.sqlite.org).

**HIGHLY EXPERIMENTAL** do not use in production as only basic tests have been run and data loss cannot be guaranteed.

## Why

This crate was built to test the feasiblity of using SQLite as an alternative to something like [tfrecord](https://www.tensorflow.org/tutorials/load_data/tfrecord) for storing a large number of data with an accessible API.  This is backed up by data from SQLite showing that it can be [faster than filesytems](https://www.sqlite.org/fasterthanfs.html) for certain data access patterns.

Although `tfrecord` and SQLite share a similar few large storage files architecture - making backups and data movement efficient/easy - having both a SQLite interface and [Amazon S3](https://aws.amazon.com/s3/)/`http` API makes management even easier with tooling that likely already exists on your machine.

## Architecture

Each bucket is saved to a separate `.sqlite3` database named after the bucket name. The [smithy](https://github.com/awslabs/smithy) generated bindings for `s3` are then mapped to the correct SQL calls against a very simple schema that is designed to be human accessible.

### data

The main table, `data`, is a simple key/value with metadata store.

```sql
CREATE TABLE IF NOT EXISTS data (
    key             TEXT PRIMARY KEY,
    value           BLOB,
    size            INTEGER NOT NULL,
    metadata        TEXT,
    last_modified   TEXT NOT NULL,
    md5             TEXT
) STRICT;
```

### multipart

For `multipart` uploads two temporary tables are used:

```sql
CREATE TABLE IF NOT EXISTS multipart_upload (
    upload_id               BLOB NOT NULL  PRIMARY KEY,
    bucket                  TEXT NOT NULL,
    key                     TEXT NOT NULL,
    last_modified           TEXT NOT NULL,
    access_key              TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS multipart_upload_part (
    upload_id               BLOB NOT NULL,
    last_modified           TEXT NOT NULL,
    part_number             INTEGER NOT NULL,
    value                   BLOB NOT NULL,
    size                    INTEGER NOT NULL,
    md5                     TEXT,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE
) STRICT;
```

## Build

This code can be used as a library or a standalone binary. To build the binary:

```bash
cargo build --release  --bin s3ite --features binary
```

## Install

```bash
cargo install --path . --features binary
```

## Run

```bash
s3ite . --access-key AKIAIOSFODNN7EXAMPLE --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```