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
- Checksums: CRC32, CRC32C, CRC64NVME, SHA-1, SHA-256, SHA-512, MD5,
  XXHash3, XXHash64, and XXHash128 for uploads, reads, copies, and multipart uploads.
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
- Encryption: SSE-S3 and SSE-KMS for object writes, multipart uploads, copies,
  and bucket defaults.
- POST form uploads: browser-form `POST Object`, SigV4 POST policy validation,
  success status responses, and success redirects.
- Auth compatibility: signed owner requests, presigned URLs, SigV2/SigV4 edge
  cases, and owner-only ACL reads or owner-equivalent ACL writes.

Out of scope surfaces include IAM, STS, SNS, S3 Select, S3 Control, torrent,
public or anonymous access, public ACL grants, bucket policies, lifecycle
transitions and restore/tiering, SSE-C/DSSE-KMS encryption, S3 logging,
object ownership controls, public access block, and multi-owner behavior.

## Compatibility Matrix

This table compares feature-level S3 coverage across AWS S3, MinIO, and
s3ite. The matrix was last reconciled on 2026-06-12.

- ✅ means supported.
- ❌ means unsupported or not implemented.
- For s3ite, ✅ means the feature is implemented in the current code and
  covered by local Rust conformance tests. Compatibility-limited support is
  called out in the notes.

### Sources

- AWS S3 surface: [Amazon S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html),
  [Amazon S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html),
  [Amazon S3 GetObjectTorrent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html),
  [Amazon S3 SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-c-encryption.html),
  [Amazon S3 SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html),
  [Amazon S3 POST Object](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html),
  [Amazon S3 object tags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html),
  [Amazon S3 bucket tags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/buckets-tagging.html),
  [Amazon S3 access points](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html),
  [Amazon S3 Multi-Region Access Points](https://docs.aws.amazon.com/en_en/AmazonS3/latest/userguide/MultiRegionAccessPoints.html),
  [Amazon S3 Object Lambda availability change](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazons3-ol-change.html),
  and [S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-getting-started.html).
- MinIO surface: [MinIO S3 API Compatibility](https://docs.min.io/aistor/developers/s3-api-compatibility/),
  [MinIO server-side encryption](https://docs.min.io/aistor/installation/linux/server-side-encryption/),
  [MinIO Object Lambda transforms](https://docs.min.io/aistor/developers/transforms-with-object-lambda/),
  and [MinIO S3 compatibility product overview](https://www.min.io/product/s3-compatibility).
- s3ite surface: this README, [src/s3](src/s3),
  [src/s3/sse.rs](src/s3/sse.rs), and [tests](tests).

### Feature Matrix

| Area | Feature | AWS S3 | MinIO | s3ite | Notes |
| --- | --- | --- | --- | --- | --- |
| Buckets | CreateBucket | ✅ | ✅ | ✅ | Basic bucket create/delete behavior. |
| Buckets | DeleteBucket | ✅ | ✅ | ✅ | Includes empty-bucket delete behavior. |
| Buckets | Delete non-empty bucket rejection | ✅ | ✅ | ✅ | s3ite returns the S3-shaped non-empty error. |
| Buckets | HeadBucket | ✅ | ✅ | ✅ | Basic existence probe. |
| Buckets | GetBucketLocation | ✅ | ✅ | ✅ | s3ite returns its fixed/default location shape. |
| Buckets | ListBuckets | ✅ | ✅ | ✅ | s3ite lists opened bucket databases. |
| Buckets | ListBuckets pagination | ✅ | ✅ | ✅ | Supports paginated ListBuckets responses. |
| Buckets | Bucket DNS naming validation | ✅ | ✅ | ✅ | s3ite validates good and bad DNS-style names. |
| Buckets | Long bucket names up to DNS limit | ✅ | ✅ | ✅ | s3ite supports 60-63 character names and long listing. |
| Buckets | Virtual-hosted-style addressing | ✅ | ✅ | ✅ | s3ite supports this when `domain_name` is configured. |
| Buckets | Expected bucket owner header behavior | ✅ | ❌ | ✅ | Validated against s3ite's synthetic owner ID `s3ite`; cross-account ownership remains out of scope. |
| Objects | PutObject | ✅ | ✅ | ✅ | Basic object write. |
| Objects | PutObject with chunked transfer encoding | ✅ | ✅ | ✅ | Includes AWS chunked content encoding behavior. |
| Objects | Missing Content-Length rejection | ✅ | ✅ | ✅ | s3ite rejects missing/invalid declared write lengths. |
| Objects | Oversized single PutObject rejection | ✅ | ✅ | ✅ | s3ite enforces its configured single-request body limit. |
| Objects | GetObject | ✅ | ✅ | ✅ | Basic object read. |
| Objects | HeadObject | ✅ | ✅ | ✅ | Metadata-only read. |
| Objects | DeleteObject | ✅ | ✅ | ✅ | Includes missing-key no-op semantics. |
| Objects | DeleteObjects | ✅ | ✅ | ✅ | Includes v1/v2 multi-delete behavior. |
| Objects | CopyObject | ✅ | ✅ | ✅ | Same-bucket and cross-bucket copies support versioned sources, metadata directives, and destination If-Match and If-None-Match conditions. |
| Objects | CopyObject from access point ARN | ✅ | ❌ | ❌ | s3ite explicitly returns `NotImplemented` for access point copy sources. |
| Objects | Range GET | ✅ | ✅ | ✅ | Includes leading/trailing/invalid/empty-object range cases. |
| Objects | Completed multipart part reads with `partNumber` | ✅ | ✅ | ✅ | s3ite supports `GetObject` and `HeadObject` part-number reads. |
| Objects | User metadata | ✅ | ✅ | ✅ | s3ite stores and returns user metadata. |
| Objects | RFC2047/unicode metadata decoding | ✅ | ✅ | ✅ | s3ite decodes RFC2047 metadata for SDK-visible metadata. |
| Objects | Cache-Control object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Expires object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Content-Type object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Content-Disposition and Content-Language object headers | ✅ | ✅ | ✅ | PUT and multipart uploads store both headers. GET and HEAD return them. CopyObject honors the metadata directive. |
| Objects | Content-Encoding object header | ✅ | ✅ | ✅ | s3ite normalizes AWS chunked encoding. |
| Objects | Response header overrides on GET | ✅ | ✅ | ✅ | s3ite supports response-header override behavior. |
| Objects | Request ID/error header compatibility | ✅ | ✅ | ✅ | s3ite emits request IDs on supported error paths. |
| Objects | GetObjectTorrent | ✅ | ❌ | ❌ | s3ite reports torrent metadata as unavailable. |
| Listings | ListObjects v1 | ✅ | ✅ | ✅ | Includes prefix, delimiter, marker, max-keys, encoding, and owner behavior. |
| Listings | ListObjectsV2 | ✅ | ✅ | ✅ | Includes continuation-token and start-after behavior. |
| Listings | Prefix filtering | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | Delimiter/common-prefix grouping | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | URL encoding in list responses | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | Unreadable/special key listing edge cases | ✅ | ✅ | ✅ | s3ite supports special prefixes and key names. |
| Listings | FetchOwner in ListObjectsV2 | ✅ | ✅ | ✅ | s3ite returns the synthetic owner. |
| Listings | ListObjectVersions | ✅ | ✅ | ✅ | Includes versions and delete markers. |
| Auth | SigV4 signed owner requests | ✅ | ✅ | ✅ | s3ite supports a single configured owner identity. |
| Auth | Presigned GET URLs | ✅ | ✅ | ✅ | Includes expiry and tamper rejection. |
| Auth | Presigned PUT URLs | ✅ | ✅ | ✅ | Includes owner-equivalent ACL compatibility. |
| Auth | SigV2 legacy compatibility edges | ❌ | ✅ | ✅ | Uses the upstream SigV2 verifier and its date and canonical path rules. |
| Auth | Unsigned public/anonymous access | ✅ | ✅ | ❌ | s3ite denies unsigned requests when auth is configured. |
| Auth | Bucket policy API and enforcement | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Auth | IAM user/group/role/account APIs | ✅ | ❌ | ❌ | MinIO has IAM concepts, but not the AWS IAM API surface. |
| Auth | STS session credentials and AssumeRole | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Auth | Cross-account authorization | ✅ | ✅ | ❌ | s3ite is single-owner. |
| ACLs | GetBucketAcl | ✅ | ❌ | ✅ | s3ite returns a synthetic owner `FULL_CONTROL` ACL. |
| ACLs | GetObjectAcl | ✅ | ❌ | ✅ | s3ite returns a synthetic owner `FULL_CONTROL` ACL. |
| ACLs | Owner-equivalent canned ACL writes | ✅ | ❌ | ✅ | s3ite accepts `private`/owner-equivalent writes as no-ops. |
| ACLs | Public canned ACL grants | ✅ | ❌ | ❌ | s3ite rejects public ACLs. MinIO documents object ACL APIs as unsupported. |
| ACLs | Explicit grant headers/XML ACL bodies | ✅ | ❌ | ❌ | s3ite rejects grant headers and ACL XML bodies. |
| ACLs | Object ownership controls | ✅ | ❌ | ❌ | MinIO documents bucket ownership controls as unsupported. |
| Versioning | GetBucketVersioning | ✅ | ✅ | ✅ | s3ite supports enabled/suspended/unset states. |
| Versioning | PutBucketVersioning enabled/suspended | ✅ | ✅ | ✅ | s3ite supports enabled and suspended. |
| Versioning | Null version behavior while suspended | ✅ | ✅ | ✅ | Includes conditional PUT and multipart completion before and after a null delete marker. |
| Versioning | Version-specific GetObject/HeadObject | ✅ | ✅ | ✅ | s3ite supports versioned reads and heads. |
| Versioning | Version-specific CopyObject | ✅ | ✅ | ✅ | s3ite supports copy by source version. |
| Versioning | Version-specific DeleteObject | ✅ | ✅ | ✅ | s3ite supports deleting explicit versions. |
| Versioning | Delete markers | ✅ | ✅ | ✅ | Includes `x-amz-delete-marker` behavior. |
| Versioning | Multi-delete with version IDs | ✅ | ✅ | ✅ | Includes delete-marker creation behavior. |
| Versioning | ListObjectVersions pagination/markers | ✅ | ✅ | ✅ | s3ite supports key/version markers. |
| Versioning | MFA Delete | ✅ | ❌ | ❌ | s3ite rejects MFA Delete configuration. |
| Multipart | CreateMultipartUpload | ✅ | ✅ | ✅ | Basic multipart initiation. |
| Multipart | UploadPart | ✅ | ✅ | ✅ | Includes size and part-number validation. |
| Multipart | CompleteMultipartUpload | ✅ | ✅ | ✅ | Includes ordering, ETag, and missing/incorrect part validation. |
| Multipart | AbortMultipartUpload | ✅ | ✅ | ✅ | Includes missing upload behavior. |
| Multipart | ListMultipartUploads | ✅ | ✅ | ✅ | s3ite supports prefix, delimiter, pagination, and owner fields. MinIO documents an exact-prefix limitation. |
| Multipart | ListParts | ✅ | ✅ | ✅ | s3ite supports pagination and ETags. |
| Multipart | UploadPartCopy | ✅ | ✅ | ✅ | Includes ranges, conditions, special names, and versions. |
| Multipart | Multipart idempotent complete retry | ✅ | ✅ | ✅ | s3ite records completion results for retries. |
| Multipart | Multipart re-upload part replacement | ✅ | ✅ | ✅ | s3ite replaces prior part rows when a part number is re-uploaded. |
| Multipart | AbortIncompleteMultipartUpload lifecycle action | ✅ | ❌ | ✅ | s3ite applies matching lifecycle abort rules during bucket maintenance; MinIO documents this lifecycle action as unsupported. |
| Checksums | Content-MD5 validation | ✅ | ✅ | ✅ | Includes invalid short, malformed, bad, empty, and missing forms. |
| Checksums | PutObject SHA256 checksum | ✅ | ✅ | ✅ | s3ite validates and stores the checksum. |
| Checksums | PutObject CRC64NVME checksum | ✅ | ✅ | ✅ | s3ite validates and stores the checksum. |
| Checksums | Stored MD5, SHA512, XXHASH3, XXHASH64, XXHASH128 | ✅ | ✅ | ✅ | PUT validates the digest, GET and HEAD return it with checksum mode enabled, and multipart uploads support composite checksums. [AWS](https://aws.amazon.com/about-aws/whats-new/2026/04/s3-five-additional-checksum-algorithms/) · [MinIO AIStor](https://docs.min.io/aistor/developers/s3-api-compatibility/#object-checksums) |
| Checksums | Multipart SHA256 checksum | ✅ | ✅ | ✅ | Includes composite checksum behavior. |
| Checksums | Multipart CRC32 checksum | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart CRC32C checksum | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart CRC64NVME checksum | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart SHA1 checksum | ✅ | ✅ | ✅ | Includes composite checksum behavior. |
| Checksums | GetObject checksum-mode response headers | ✅ | ✅ | ✅ | s3ite returns stored checksums when requested. |
| Checksums | CopyObject checksum inheritance/defaulting | ✅ | ✅ | ✅ | s3ite supports inherited and default checksum algorithms. |
| Checksums | Default CRC64NVME when checksum omitted | ✅ | ❌ | ✅ | s3ite intentionally mirrors current AWS default behavior here. |
| Attributes | GetObjectAttributes basic metadata | ✅ | ✅ | ✅ | ETag, size, storage class, checksum. |
| Attributes | GetObjectAttributes versioned objects | ✅ | ✅ | ✅ | Supported by s3ite. |
| Attributes | GetObjectAttributes multipart parts | ✅ | ✅ | ✅ | Includes part pagination. |
| Attributes | GetObjectAttributes multipart part checksums | ✅ | ✅ | ✅ | Includes checksum attributes for parts. |
| Encryption | Explicit SSE-S3 PutObject (`AES256`) | ✅ | ✅ | ✅ | s3ite encrypts stored bytes and returns plaintext on read. An empty algorithm returns `400 InvalidArgument`. |
| Encryption | SSE-S3 HeadObject/GetObject response headers | ✅ | ✅ | ✅ | s3ite returns `AES256` for encrypted objects. |
| Encryption | Bucket default SSE-S3 config | ✅ | ✅ | ✅ | Supports Put/Get/DeleteBucketEncryption for SSE-S3. Malformed configurations return `400 MalformedXML`. |
| Encryption | SSE-S3 multipart upload | ✅ | ✅ | ✅ | Works with default bucket encryption and multipart writes. |
| Encryption | SSE-S3 CopyObject | ✅ | ✅ | ✅ | Supports plaintext/encrypted source and destination permutations. |
| Encryption | SSE-S3 UploadPartCopy | ✅ | ✅ | ✅ | Supports plaintext/encrypted source and destination permutations. |
| Encryption | SSE-KMS object encryption | ✅ | ✅ | ✅ | PUT, signed browser POST, multipart, and copy support `aws:kms` with an explicit key ID. |
| Encryption | Bucket default SSE-KMS | ✅ | ✅ | ✅ | Put/Get/DeleteBucketEncryption manage a default KMS key for new writes. |
| Encryption | SSE-C object encryption | ✅ | ✅ | ❌ | s3ite rejects SSE-C headers as unsupported. |
| Encryption | SSE-C multipart | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Encryption | S3 Bucket Keys | ✅ | ❌ | ❌ | s3ite rejects bucket-key settings. |
| Encryption | DSSE-KMS | ✅ | ❌ | ❌ | Not implemented by s3ite. |
| Encryption | Encryption conflict validation | ✅ | ✅ | ✅ | s3ite rejects mixed SSE-C/SSE-KMS/SSE-S3 request shapes. |
| Tagging | Bucket tagging API | ✅ | ✅ | ✅ | Supports Get/Put/DeleteBucketTagging. |
| Tagging | Object tagging API | ✅ | ✅ | ✅ | Supports Get/Put/DeleteObjectTagging with version-specific tags. |
| Tagging | Object tags on PutObject | ✅ | ✅ | ✅ | Supports PutObject tags, CopyObject tag directives, and multipart upload tags. A tag-only self-copy returns `400 InvalidRequest`. |
| Tagging | Object tags on POST Object | ✅ | ✅ | ✅ | POST Object forwards `x-amz-tagging` through the normal object write path. |
| Tagging | Tag-based bucket policy conditions | ✅ | ✅ | ❌ | Requires bucket policy support. |
| Lifecycle | Put/Get/DeleteBucketLifecycle | ✅ | ✅ | ✅ | s3ite stores normalized lifecycle rules in each bucket DB. |
| Lifecycle | Current object expiration | ✅ | ✅ | ✅ | Applied during startup and periodic bucket maintenance. |
| Lifecycle | Noncurrent version expiration | ✅ | ✅ | ✅ | Supports `NoncurrentDays` and `NewerNoncurrentVersions`. |
| Lifecycle | Delete-marker expiration | ✅ | ✅ | ✅ | Removes expired delete markers when they are the only remaining version. |
| Lifecycle | Lifecycle filters by prefix/tag/size | ✅ | ✅ | ✅ | Supports legacy prefix, filter prefix, tag, size, and `And` filters. |
| Lifecycle | Storage-class transitions | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | RestoreObject from archive/tier | ✅ | ✅ | ❌ | MinIO lists `RestoreObject`; s3ite has no restore/tiering. |
| Lifecycle | Restore status in list outputs | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| CORS | Bucket CORS API | ✅ | ✅ | ✅ | s3ite stores per-bucket CORS rules in each bucket DB; current MinIO docs list `PutBucketCors` and `DeleteBucketCors`. |
| CORS | CORS preflight behavior | ✅ | ✅ | ✅ | s3ite applies bucket CORS first and falls back to configurable server-level CORS when no bucket config exists. |
| POST | POST Object form uploads | ✅ | ✅ | ✅ | Browser form uploads reuse the normal object write path. |
| POST | POST Object policy validation | ✅ | ✅ | ✅ | SigV4 POST policy validation is handled by `s3s`. |
| POST | POST Object success redirects/status codes | ✅ | ✅ | ✅ | Supports default 204 responses, 200/201 statuses, and 303 redirects. |
| Object Lock | Bucket Object Lock configuration | ✅ | ✅ | ✅ | Enable Object Lock at creation or later on an existing bucket with versioning enabled. |
| Object Lock | Object retention governance/compliance | ✅ | ✅ | ✅ | Includes default retention, explicit retention, extension, and active-retention shortening rules. |
| Object Lock | Object legal hold | ✅ | ✅ | ✅ | Legal hold is stored per object version and exposed through the S3 APIs. |
| Object Lock | Delete blocked by retention/legal hold | ✅ | ✅ | ✅ | Exact-version deletes are blocked by legal hold, compliance retention, and governance retention unless bypass is supplied. |
| Object Lock | Retention metadata on HeadObject/GetObject | ✅ | ✅ | ✅ | s3ite returns Object Lock mode, retain-until date, and legal-hold headers on read metadata. |
| Policy/Security | PublicAccessBlock APIs | ✅ | ❌ | ❌ | MinIO documents these APIs as unsupported. |
| Policy/Security | Bucket policy status API | ✅ | ✅ | ❌ | Requires bucket policy support. |
| Policy/Security | Requester Pays / request payment | ✅ | ❌ | ❌ | MinIO documents request-payment APIs as unsupported. |
| Notifications | Bucket notification configuration | ✅ | ✅ | ❌ | MinIO documents bucket notifications; s3ite does not implement them. |
| Notifications | SNS-compatible notifications | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Replication | Bucket replication configuration | ✅ | ✅ | ❌ | MinIO documents bucket replication; s3ite does not implement replication. |
| Logging | S3 server access logging | ✅ | ❌ | ❌ | MinIO documents bucket logging APIs as unsupported; s3ite exports audit events over OTLP. |
| Website | Static website bucket configuration | ✅ | ❌ | ❌ | MinIO documents website APIs as unsupported. |
| Inventory | Bucket inventory configuration | ✅ | ❌ | ❌ | MinIO documents inventory APIs as unsupported. |
| Analytics | Bucket analytics configuration | ✅ | ❌ | ❌ | MinIO documents some analytics operations but still lists `GetBucketAnalyticsConfiguration` as unsupported, so full configuration parity is not present. |
| Metrics | Bucket metrics configuration | ✅ | ❌ | ❌ | s3ite exports operational metrics over OTLP. |
| Accelerate | Bucket accelerate configuration | ✅ | ❌ | ❌ | MinIO documents accelerate APIs as unsupported. |
| Storage class | STANDARD storage-class reporting | ✅ | ✅ | ✅ | s3ite reports `STANDARD` in list/object-attribute shapes. |
| Storage class | REDUCED_REDUNDANCY PutObject validation | ✅ | ✅ | ✅ | s3ite accepts this request value but does not implement tiered placement. |
| Storage class | Multiple durable storage classes | ✅ | ✅ | ❌ | s3ite has one SQLite-backed placement mode. |
| Storage class | Glacier/Deep Archive restore workflow | ✅ | ✅ | ❌ | Requires lifecycle/tiering/restore. |
| Advanced | SelectObjectContent / S3 Select | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Advanced | S3 Control account public access block | ✅ | ❌ | ❌ | AWS managed control-plane feature; not a s3ite surface. |
| Advanced | Access Points | ✅ | ❌ | ❌ | s3ite rejects access point copy sources and has no access point routing. |
| Advanced | Multi-Region Access Points | ✅ | ❌ | ❌ | AWS-only managed service feature. |
| Advanced | S3 on Outposts | ✅ | ❌ | ❌ | AWS-only managed service feature. |
| Advanced | Directory buckets / S3 Express One Zone | ✅ | ❌ | ❌ | s3ite has ordinary bucket databases only. |
| Advanced | S3 Object Lambda transforms | ✅ | ✅ | ❌ | AWS marks S3 Object Lambda as existing-customer/APN-limited after 2025-11-07; MinIO has Object Lambda transform docs; s3ite does not implement transforms. |
| Advanced | Object Lambda AWS control APIs | ✅ | ❌ | ❌ | AWS managed control-plane feature. |
| Advanced | S3 Tables APIs | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Tables separately; not s3ite. |
| Advanced | S3 Vectors APIs | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Vectors separately; not s3ite. |
| Advanced | S3 Files APIs | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Files separately; not s3ite. |

### s3ite Gaps To Track

The largest compatibility gaps relative to AWS S3 and MinIO are:

- Auth and authorization: bucket policies, public/anonymous access, IAM, STS,
  cross-account behavior, and full ACL grant enforcement.
- Object-management APIs: lifecycle transitions, restore/tiering, PublicAccessBlock,
  notifications, logging, website hosting, inventory, analytics, metrics, and
  request payment.
- Encryption variants: SSE-C, DSSE-KMS, and S3 Bucket Keys.
- AWS managed/control-plane features: S3 Control, access points,
  Multi-Region Access Points, S3 Express/directory buckets, S3 on Outposts,
  Object Lambda control APIs, S3 Tables, S3 Vectors, and S3 Files.

s3ite's implemented surface is concentrated on the data path: bucket lifecycle,
object CRUD, listings, versioning, multipart, checksums, owner-only auth/ACL
compatibility, selected raw HTTP edge cases, SSE-S3, and SSE-KMS.

## Storage Model

Each bucket is stored in one SQLite database named `<bucket>.sqlite3`. Bucket
databases are opened at startup from the configured root path, and new buckets
create new database files in that same directory.

The 0.7.0 bucket schema stores the integer `70` in `PRAGMA user_version`.
Startup checks every bucket, including read-only buckets, and rejects unsupported
schema versions before serving requests. To inspect an existing bucket, run
`sqlite3 -readonly path/to/bucket.sqlite3 'PRAGMA user_version;'`.
Tables are `STRICT`, use normal rowid tables, and include foreign keys and
indexes for the storage queries used by the S3 handlers.

Each bucket database stores object data and upload state in these tables.

- `object_version` stores keys, version IDs, delete-marker state, latest markers,
  object metadata, size, ETag, checksums, and timestamps.
- `object_part` records part numbers, sizes, and optional per-part checksums
  for pending uploads and stored objects. For stored objects it also records
  where each part starts.
- `object_blob_chunk` stores the bytes of each part in bounded BLOB chunks,
  linked to either a pending upload or an object version.
- `multipart_upload` tracks in-progress uploads. `completed_multipart_upload`
  records completion results so clients can safely retry.
- `bucket_state` stores bucket-level state such as versioning as an integer enum.

Writes are performed transactionally. Larger request work such as multipart
normalization and checksum calculation is kept outside SQLite write transactions
where practical, so the single writer is held for the database mutation itself.

### SSE-KMS encryption

Set `kms_region` in YAML or pass `--kms-region us-east-1` to enable SSE-KMS.
s3ite uses the AWS SDK credential chain for its own KMS identity. Grant that
identity `kms:GenerateDataKey` and `kms:Decrypt` on the keys clients may use.
For an AWS KMS-compatible service, set `AWS_ENDPOINT_URL_KMS` on the server.

For example, write an object with a symmetric KMS key in the configured region:

```sh
aws --endpoint-url http://localhost:8014 s3api put-object \
  --bucket example --key report --body ./report \
  --server-side-encryption aws:kms --ssekms-key-id alias/object-storage
```

Clients can also set `aws:kms` and a key ID as the bucket default. Each new
object or multipart upload gets a fresh AES-256 data key. SQLite stores the
encrypted chunks, wrapped data key, canonical KMS key ID, and encryption
context. The context includes the destination object's S3 ARN and can include
client-supplied string pairs. Reading an object requires access to its original
KMS key, including after the bucket default changes or a database is restored.
Copies use the destination encryption settings. Multipart uploads keep the key
selected at initiation.

## Migrate from v0.6.0 over HTTP

Start the v0.6.0 server with its existing data directory and `--read-only true`
so the source stays consistent. Start the new server on a separate endpoint
with its own data directory. The migration script creates missing buckets,
including empty ones, and copies the current object at each key with its user
metadata. Allow enough temporary disk space on the machine running the script
for the largest source object. [Migration script](scripts/migration.py)

From the repository root, using the URLs for your two servers, run

```bash
python3 -m venv .venv
.venv/bin/python -m pip install 'boto3>=1.36'
.venv/bin/python scripts/migration.py \
  --source http://localhost:8000 \
  --destination http://localhost:8001
```

Both endpoints use the standard AWS credential chain, including
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. If the servers use different
credentials, add `--source-profile old --destination-profile new`, replacing
`old` and `new` with your configured AWS profile names.

The script checks a source object's stored MD5 ETag when available and computes
SHA-256 over its bytes. The destination checks that SHA-256 during upload. The
script then reads the object back and compares its hash and metadata. On a
rerun, it skips an existing key only after the same comparison. A conflict
stops the run and preserves the destination object.

The destination assigns fresh modification timestamps. The transfer covers
live objects and empty buckets. Bucket configuration, historical versions, and
unfinished multipart uploads are outside its scope. A v0.6.0 bucket containing
a directory marker (an empty object with a key ending in `/`) can return
`InternalError` on listing. The script reports the affected bucket and exits.

## Auth Model

When `access_key` and `secret_key` are configured, signed requests and valid
presigned URLs from that identity act as the single owner. Unsigned requests are
denied. When both values are omitted, authentication is disabled and every
request behaves as that owner. Startup fails if only one credential is set.

SigV2 header authentication follows `s3s`. Send an RFC 1123 HTTP date in
`Date` or a UTC timestamp such as `20261117T120000Z` in `x-amz-date`.
Sign the request path exactly as sent, including any trailing slash.
The server rejects malformed dates and checks clock skew.

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
- `blob_chunk_size_mib`: size of each stored object chunk, in MiB. Each
  in-flight upload buffers at most one chunk (two while encrypting), so peak
  upload memory is about `concurrency_limit` times this value. A copy also holds
  one chunk of its source, which can be larger if the source was stored with a
  bigger setting: existing objects keep their chunk sizes when it changes.
  Default: `16`.
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

### Health probes

Enable the dedicated HTTP listener with `--health-bind 127.0.0.1:8015` or
`health_bind: 127.0.0.1:8015` in YAML. It is off by default. For a container
probe, choose an address the orchestrator can reach. Both endpoints accept
unauthenticated `GET` and `HEAD` requests and return an empty body.

| Endpoint | During bucket initialization | Accepting S3 requests | Draining active requests |
| --- | --- | --- | --- |
| `/livez` | 200 | 200 | 200 |
| `/readyz` | 503 | 200 | 503 |

Readiness turns on after bucket initialization and S3 socket binding. It turns
off when graceful shutdown begins. Probes report the server lifecycle without
issuing SQLite queries or calling KMS.

### OpenTelemetry

Export is off by default. To send traces, metrics, and audit logs to one
OTLP/HTTP collector, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318`.
The SDK appends the signal path. To enable individual signals, configure their
full URLs in YAML or use the corresponding CLI flags.

| Signal | YAML setting | CLI flag | Path |
| --- | --- | --- | --- |
| Traces | `otlp_traces_endpoint` | `--otlp-traces-endpoint` | `/v1/traces` |
| Metrics | `otlp_metrics_endpoint` | `--otlp-metrics-endpoint` | `/v1/metrics` |
| Audit logs | `otlp_logs_endpoint` | `--otlp-logs-endpoint` | `/v1/logs` |

The SDK also accepts `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT`.
Explicit CLI and YAML endpoints take precedence. Set an individual selector
such as `OTEL_LOGS_EXPORTER=none` to disable that signal or `otlp` to enable it.
`OTEL_SDK_DISABLED=true` disables all three. The default protocol is
HTTP/protobuf. Set `OTEL_EXPORTER_OTLP_PROTOCOL=http/json` for JSON or use the
signal-specific protocol variable. `OTEL_SERVICE_NAME` defaults to `s3ite`.

**Traces.** Each HTTP request creates a server span with S3 operation spans
beneath it. s3ite propagates W3C `traceparent`, `tracestate`, and `baggage`.
The default trace sampler follows the caller’s sampling decision. `RUST_LOG`
controls console logging independently.

**Audit logs.** The `s3.request` event records mutations and failed requests.
Set `audit_reads: true` or `--audit-reads true` to include successful reads and
listings. Records include the request ID, operation, HTTP status, and, once
the request reaches an S3 handler, its available principal and target fields.
Requests rejected before dispatch carry the URL path without its query.
A governance bypass header is recorded as a request for bypass. Each committed
bulk delete also emits `s3.delete_object.result` per item, including failures
and quiet-mode successes. The item records share the request ID returned to the
client. Audit records include trace context when available, independently of
the caller's trace sampling decision. The exporter buffers records in memory
and flushes on graceful shutdown. A process crash, a full queue, or an
unavailable collector can cause gaps.

**Metrics.** The `s3ite.request.duration` histogram measures seconds until
response headers by S3 operation and HTTP status. Use its count for request
and error rates. Requests that never reached a handler use `unrouted`.
`s3ite.sqlite.writer.queue.duration` measures write transaction queue wait.
`s3ite.kms.request.duration` measures KMS calls including SDK retries, by
operation and success. On Unix, `s3ite.storage.available` reports available
bytes on the filesystem containing the storage root. Metric labels omit
bucket names, object keys, principals, and request IDs.

### CORS

S3 bucket CORS APIs are supported and store rules in the bucket database.
When a request includes an Origin header and the bucket has CORS rules, the
origin and HTTP method must match a rule. A mismatch returns 403 before the
operation runs. This also applies to presigned URLs.
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

The Rust conformance suite includes scenarios ported from or inspired by
Ceph's S3 tests. Ceph maintains its suite under
`src/test/rgw/s3-tests` in `ceph/ceph`. Source paths in the Rust test comments
are relative to that directory. The SSE-KMS cases run against a local mock that
serves signed AWS KMS requests. The presigned expiry, SSE-KMS, Unicode metadata, and
POST policy expectations were checked against [Ceph revision 6ffb1fc](https://github.com/ceph/ceph/tree/6ffb1fc533b38a153f91b46eba6deb3ec3fa5788/src/test/rgw/s3-tests).

Run all tests:

```bash
cargo test
```

Useful focused runs:

```bash
cargo test --test conformance_bucket
cargo test --test conformance_kms
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
