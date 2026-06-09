# s3ite

`s3ite` is an S3-compatible HTTP server backed by SQLite. It is a binary-only
crate: run the server, point S3 clients at it, and store each bucket as a
separate `.sqlite3` database under the configured root directory.

The project is still experimental. It has Rust conformance tests, storage
transaction tests, log tests, and a per-file coverage gate, but it is not a
drop-in replacement for Amazon S3 or a production durability promise.

## What It Is For

`s3ite` was built to make SQLite usable as a compact object store with an API
that normal S3 tooling already understands. The original motivation was machine
learning data storage: SQLite gives you a few movable files, transactional
updates, queryable metadata, and BLOB storage, while the S3 surface lets existing
SDKs and command-line tools read and write the data.

## Supported Surface

The supported S3 surface is intentionally focused:

- Buckets: create, head, list, delete, location, and versioning state.
- Objects: put, get, head, delete, multi-delete, copy, ranges, conditionals,
  metadata, and HTTP headers.
- Listings: `ListObjects`, `ListObjectsV2`, and `ListObjectVersions`, including
  prefixes, delimiters, pagination, URL encoding, owners, delete markers, and
  version IDs.
- Multipart: create, upload part, upload part copy, list parts, list uploads,
  complete, abort, completed-part reads, and idempotent completion responses.
- Checksums: `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, and `SHA256` request,
  storage, copy, multipart, and response behavior.
- Tagging: bucket tag create/read/replace/delete, object tag
  create/read/replace/delete, version-specific object tags, `PutObject` tags,
  `CopyObject` tag directives, multipart upload tags, and POST form upload
  tags.
- Object Lock: bucket Object Lock configuration, default retention, per-version
  retention and legal hold metadata, read headers, copy/multipart propagation,
  and delete enforcement for governance/compliance/legal hold.
- Lifecycle: bucket lifecycle configuration, current object expiration,
  noncurrent version expiration, expired delete-marker cleanup, prefix/tag/size
  filters, and incomplete multipart upload aborts.
- POST form uploads: browser-form `POST Object`, SigV4 POST policy validation,
  success status responses, and success redirects.
- Auth compatibility: signed owner requests, presigned URLs, SigV2/SigV4 edge
  cases, and owner-only ACL reads or owner-equivalent ACL writes.

Out of scope surfaces include IAM, STS, SNS, S3 Select, S3 Control, torrent,
public or anonymous access, public ACL grants, bucket policies, lifecycle
transitions and restore/tiering, SSE-KMS/SSE-C/DSSE-KMS encryption, S3 logging,
object ownership controls, public access block, and multi-owner behavior.

## Storage Model

Each bucket is stored in one SQLite database named `<bucket>.sqlite3`. Bucket
databases are opened at startup from the configured root path, and new buckets
create new database files in that same directory.

The current bucket schema is versioned and validated on open. Unsupported older
schemas are rejected rather than migrated in place. Tables are `STRICT`, use
normal rowid tables, and include foreign keys and indexes for the storage queries
used by the S3 handlers.

Object metadata and object bodies are split:

- `object_version` stores keys, version IDs, delete-marker state, latest markers,
  object metadata, size, ETag, checksums, and timestamps.
- `object_body` stores the object BLOB by `object_version.sequence`.
- `completed_object_part` stores completed multipart boundaries and per-part
  checksum metadata.
- `multipart_upload`, `multipart_upload_part`, and
  `completed_multipart_upload` track in-progress uploads, uploaded parts, and
  idempotent completion responses.
- `bucket_state` stores bucket-level state such as versioning as an integer enum.

Writes are performed transactionally. Larger request work such as multipart
normalization and checksum calculation is kept outside SQLite write transactions
where practical, so the single writer is held for the database mutation itself.

## Auth Model

When `access_key` and `secret_key` are configured, signed requests and valid
presigned URLs from that identity act as the single owner. Unsigned requests are
denied. When both values are omitted, authentication is disabled and every
request behaves as that owner.

ACLs are compatibility-only. `GetBucketAcl` and `GetObjectAcl` return a
synthetic owner `FULL_CONTROL` ACL, and owner-equivalent ACL writes such as
`private` are accepted as no-ops. ACL XML bodies, explicit grant headers, public
ACLs, IAM, bucket policies, ownership controls, and anonymous public access are
not permission mechanisms in `s3ite`.

## Build And Run

Use a Rust toolchain compatible with edition 2024.

```bash
cargo build --release
```

Run locally:

```bash
cargo run -- \
  --root ./data \
  --host 0.0.0.0 \
  --port 8014 \
  --access-key AKIAIOSFODNN7EXAMPLE \
  --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Install from the checkout:

```bash
cargo install --path .
```

Build the Docker image:

```bash
docker build -t s3ite:latest .
```

Run the Docker image:

```bash
docker run --rm \
  -e RUST_LOG=info \
  -v "$(pwd)/data:/data" \
  -p 8014:8014 \
  s3ite:latest \
  --root /data \
  --host 0.0.0.0 \
  --port 8014 \
  --access-key AKIAIOSFODNN7EXAMPLE \
  --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Configuration

Configuration can come from defaults, a YAML file passed with `--config`, and
command-line overrides. Command-line values override YAML service-level values.
YAML additionally supports per-bucket overrides.

Common service options:

- `root`: directory containing bucket `.sqlite3` files. Default: `.`.
- `host`: bind address. Default: `0.0.0.0`.
- `port`: bind port. Default: `8014`.
- `access_key` and `secret_key`: enable owner request authentication.
- `domain_name`: enables virtual-hosted-style bucket parsing for that domain.
- `concurrency_limit`: max concurrent HTTP requests. Default: `16`.
- `read_only`: rejects write operations by default. Default: `false`.
- `readers`: short metadata/query SQLite reader connections. Default: `4`.
- `blob_readers`: long-lived `GetObject` BLOB reader connections. Default: `16`.
- `blob_read_timeout_seconds`: timeout for blocked BLOB readers. Default: `120`.
- `maintenance_interval_seconds`: background maintenance interval. Default: `60`.
- `maintenance_vacuum_pages`: incremental vacuum pages per pass. Default: `100`.
- `multipart_upload_ttl_seconds`: inactive incomplete upload TTL. Default: `86400`.
- `completed_multipart_upload_ttl_seconds`: completion ledger TTL. Default:
  `86400`.

SQLite options can be set globally or per bucket:

- `journal_mode`: `delete`, `truncate`, `persist`, `memory`, `wal`, or `off`.
  Default: `wal`.
- `synchronous`: `off`, `normal`, `full`, or `extra`. Default: `normal`.
- `temp_store`: `default`, `file`, or `memory`. Default: `memory`.
- `cache_size`: SQLite cache size in KiB. Default: `65536`.
- `busy_timeout_milliseconds`: SQLite busy timeout. Default: `5000`.
- `journal_size_limit`: maximum rollback journal or WAL size retained after
  reset, in bytes. Default: `67108864`.

Example YAML:

```yaml
root: /data
host: 0.0.0.0
port: 8014
access_key: AKIAIOSFODNN7EXAMPLE
secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
domain_name: s3ite.local
read_only: false

journal_mode: wal
synchronous: normal
temp_store: memory
cache_size: 65536
busy_timeout_milliseconds: 5000
journal_size_limit: 67108864

cors:
  mode: custom
  allow_origins:
    - https://app.example.com
  allow_methods:
    - GET
    - HEAD
    - PUT
    - POST
    - DELETE
  allow_headers:
    - authorization
    - content-type
    - x-amz-content-sha256
    - x-amz-date
  expose_headers:
    - ETag
    - x-amz-request-id
    - x-amz-version-id
  allow_credentials: true
  allow_private_network: false
  max_age_seconds: 600

buckets:
  training-data:
    read_only: true
    sqlite:
      cache_size: 131072
      synchronous: full
      busy_timeout_milliseconds: 10000
```

### CORS

S3 bucket CORS APIs are supported and store rules in the bucket database.
Server-level CORS remains the fallback for buckets without a bucket CORS
configuration and for non-bucket requests.

`cors.mode` can be:

- `disabled`: do not add CORS response headers.
- `permissive`: mirror request origins, methods, and headers and allow
  credentials. This is the default compatibility mode.
- `custom`: use explicit `allow_origins`, `allow_methods`, `allow_headers`,
  `expose_headers`, `allow_credentials`, `allow_private_network`, and
  `max_age_seconds`.

For custom CORS, `allow_origins` is required. The list value `["*"]` means any
value for origins, methods, headers, or exposed headers, but wildcard values
cannot be mixed with explicit values. Wildcards are also rejected when
`allow_credentials` is enabled.

The old `permissive_cors` setting remains as a deprecated compatibility alias
for `cors.mode`.

## Tests

The conformance suite is now native Rust. Many scenarios were ported from or
inspired by [ceph/s3-tests](https://github.com/ceph/s3-tests), and the Rust
tests keep per-test comments for that traceability. Thanks to the Ceph
`s3-tests` maintainers for making that compatibility surface concrete. The old
local shell wrapper for running the external suite is no longer in the
repository.

Run all tests:

```bash
cargo test
```

Useful focused runs:

```bash
cargo test --test conformance_bucket
cargo test --test storage_transactions
cargo test --test logging
```

Run the project checks used in CI:

```bash
cargo fmt --all --check
cargo clippy --all-targets --workspace -- -D warnings
cargo llvm-cov --all-targets --workspace \
  --fail-under-lines 95 \
  --fail-under-file-lines 95
```

Integration tests spawn the real `s3ite` binary through Cargo's
`CARGO_BIN_EXE_s3ite` support. Set `S3ITE_TEST_BIN` only when you need to point
tests at a separately built binary.
