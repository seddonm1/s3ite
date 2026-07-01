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
| Objects | CopyObject | ✅ | ✅ | ✅ | Same-bucket, cross-bucket, versioned, metadata-copy, and metadata-replace behavior. |
| Objects | CopyObject from access point ARN | ✅ | ❌ | ❌ | s3ite explicitly returns `NotImplemented` for access point copy sources. |
| Objects | Range GET | ✅ | ✅ | ✅ | Includes leading/trailing/invalid/empty-object range cases. |
| Objects | Completed multipart part reads with `partNumber` | ✅ | ✅ | ✅ | s3ite supports `GetObject` and `HeadObject` part-number reads. |
| Objects | User metadata | ✅ | ✅ | ✅ | s3ite stores and returns user metadata. |
| Objects | RFC2047/unicode metadata decoding | ✅ | ✅ | ✅ | s3ite decodes RFC2047 metadata for SDK-visible metadata. |
| Objects | Cache-Control object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Expires object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Content-Type object header | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
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
| Auth | SigV2 legacy compatibility edges | ❌ | ✅ | ✅ | s3ite keeps SigV2 edge behavior for legacy client compatibility. |
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
| Versioning | Null version behavior while suspended | ✅ | ✅ | ✅ | s3ite preserves null-version behavior across suspended versioning writes. |
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
| Encryption | Explicit SSE-S3 PutObject (`AES256`) | ✅ | ✅ | ✅ | s3ite encrypts stored bytes and returns plaintext on read. |
| Encryption | SSE-S3 HeadObject/GetObject response headers | ✅ | ✅ | ✅ | s3ite returns `AES256` for encrypted objects. |
| Encryption | Bucket default SSE-S3 config | ✅ | ✅ | ✅ | s3ite supports Put/Get/DeleteBucketEncryption for SSE-S3. |
| Encryption | SSE-S3 multipart upload | ✅ | ✅ | ✅ | Works with default bucket encryption and multipart writes. |
| Encryption | SSE-S3 CopyObject | ✅ | ✅ | ✅ | Supports plaintext/encrypted source and destination permutations. |
| Encryption | SSE-S3 UploadPartCopy | ✅ | ✅ | ✅ | Supports plaintext/encrypted source and destination permutations. |
| Encryption | SSE-KMS object encryption | ✅ | ✅ | ❌ | s3ite rejects KMS headers as unsupported. |
| Encryption | Bucket default SSE-KMS | ✅ | ✅ | ❌ | s3ite only supports bucket-default SSE-S3. |
| Encryption | SSE-C object encryption | ✅ | ✅ | ❌ | s3ite rejects SSE-C headers as unsupported. |
| Encryption | SSE-C multipart | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Encryption | S3 Bucket Keys | ✅ | ❌ | ❌ | s3ite rejects bucket-key settings. |
| Encryption | DSSE-KMS | ✅ | ❌ | ❌ | Not implemented by s3ite. |
| Encryption | Encryption conflict validation | ✅ | ✅ | ✅ | s3ite rejects mixed SSE-C/SSE-KMS/SSE-S3 request shapes. |
| Tagging | Bucket tagging API | ✅ | ✅ | ✅ | Supports Get/Put/DeleteBucketTagging. |
| Tagging | Object tagging API | ✅ | ✅ | ✅ | Supports Get/Put/DeleteObjectTagging with version-specific tags. |
| Tagging | Object tags on PutObject | ✅ | ✅ | ✅ | Supports PutObject tags, CopyObject tag directives, and multipart upload tags. |
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
| Object Lock | Bucket Object Lock configuration | ✅ | ✅ | ✅ | s3ite supports configuration for buckets created with Object Lock enabled. |
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
| Logging | S3 server access logging | ✅ | ❌ | ❌ | MinIO documents bucket logging APIs as unsupported; s3ite has application logs only. |
| Website | Static website bucket configuration | ✅ | ❌ | ❌ | MinIO documents website APIs as unsupported. |
| Inventory | Bucket inventory configuration | ✅ | ❌ | ❌ | MinIO documents inventory APIs as unsupported. |
| Analytics | Bucket analytics configuration | ✅ | ❌ | ❌ | MinIO documents some analytics operations but still lists `GetBucketAnalyticsConfiguration` as unsupported, so full configuration parity is not present. |
| Metrics | Bucket metrics configuration | ✅ | ❌ | ❌ | MinIO documents metrics APIs as unsupported. |
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
- Encryption variants: SSE-KMS, SSE-C, DSSE-KMS, and S3 Bucket Keys.
- AWS managed/control-plane features: S3 Control, access points,
  Multi-Region Access Points, S3 Express/directory buckets, S3 on Outposts,
  Object Lambda control APIs, S3 Tables, S3 Vectors, and S3 Files.

s3ite's implemented surface is concentrated on the data path: bucket lifecycle,
object CRUD, listings, versioning, multipart, checksums, owner-only auth/ACL
compatibility, selected raw HTTP edge cases, and SSE-S3.

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
