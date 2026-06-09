# S3 Compatibility Matrix

This document compares feature-level S3 coverage across:

- `Ceph/s3-tests`: whether the upstream Ceph S3 compatibility test suite has
  functional test coverage for the feature.
- `AWS S3`: whether the feature is part of the current Amazon S3 service/API
  surface.
- `MinIO`: whether MinIO documents support for the equivalent S3 feature/API.
- `s3ite`: whether this repository currently implements and tests the feature.

The matrix was last reconciled on 2026-06-08. It uses colored status markers in
the feature columns:

- ✅ means covered/supported.
- ❌ means not covered/unsupported.

For `Ceph/s3-tests`, ✅ means "covered by the test suite", not "implemented by
Ceph RGW". For `s3ite`, ✅ means the feature is implemented in the current code
and covered by local Rust conformance tests; where support is intentionally
compatibility-limited, the note says so.

## Sources

- Ceph test coverage: local checkout under
  `/tmp/s3ite-s3-tests-inspect/s3tests/functional`, local audit table
  `/tmp/s3ite-s3tests-scope.tsv`, and the upstream
  [ceph/s3-tests](https://github.com/ceph/s3-tests) repository.
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
- MinIO surface: [MinIO S3 API Compatibility](https://minio.community/community/minio-object-store/reference/s3-api-compatibility.html),
  [MinIO server-side encryption](https://docs.min.io/aistor/installation/linux/server-side-encryption/),
  [MinIO Object Lambda transforms](https://docs.min.io/enterprise/aistor-object-store/developers/transforms-with-object-lambda/),
  and [MinIO S3 compatibility product overview](https://www.min.io/product/s3-compatibility).
- s3ite surface: [README.md](../README.md), [src/s3](../src/s3),
  [src/s3/sse.rs](../src/s3/sse.rs), and [tests](../tests).

## Feature Matrix

| Area | Feature | Ceph/s3-tests | AWS S3 | MinIO | s3ite | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Buckets | CreateBucket | ✅ | ✅ | ✅ | ✅ | Covered by `test_bucket_create_delete` and `tests/conformance_bucket.rs`. |
| Buckets | DeleteBucket | ✅ | ✅ | ✅ | ✅ | Includes empty-bucket delete behavior. |
| Buckets | Delete non-empty bucket rejection | ✅ | ✅ | ✅ | ✅ | s3ite returns the S3-shaped non-empty error. |
| Buckets | HeadBucket | ✅ | ✅ | ✅ | ✅ | Basic existence probe. |
| Buckets | GetBucketLocation | ✅ | ✅ | ✅ | ✅ | s3ite returns its fixed/default location shape. |
| Buckets | ListBuckets | ✅ | ✅ | ✅ | ✅ | s3ite lists opened bucket databases. |
| Buckets | ListBuckets pagination | ✅ | ✅ | ✅ | ✅ | Covered locally by `test_list_buckets_paginated`. |
| Buckets | Bucket DNS naming validation | ✅ | ✅ | ✅ | ✅ | s3ite covers good and bad DNS-style names. |
| Buckets | Long bucket names up to DNS limit | ✅ | ✅ | ✅ | ✅ | s3ite covers 60-63 character names and long listing. |
| Buckets | Virtual-hosted-style addressing | ❌ | ✅ | ✅ | ✅ | s3ite supports this when `domain_name` is configured. |
| Buckets | Expected bucket owner header behavior | ✅ | ✅ | ❌ | ✅ | Validated against s3ite's synthetic owner ID `s3ite`; cross-account ownership remains out of scope. |
| Objects | PutObject | ✅ | ✅ | ✅ | ✅ | Basic object write. |
| Objects | PutObject with chunked transfer encoding | ✅ | ✅ | ✅ | ✅ | Includes AWS chunked content encoding behavior. |
| Objects | Missing Content-Length rejection | ✅ | ✅ | ✅ | ✅ | s3ite rejects missing/invalid declared write lengths. |
| Objects | Oversized single PutObject rejection | ✅ | ✅ | ✅ | ✅ | s3ite enforces its configured single-request body limit. |
| Objects | GetObject | ✅ | ✅ | ✅ | ✅ | Basic object read. |
| Objects | HeadObject | ✅ | ✅ | ✅ | ✅ | Metadata-only read. |
| Objects | DeleteObject | ✅ | ✅ | ✅ | ✅ | Includes missing-key no-op semantics. |
| Objects | DeleteObjects | ✅ | ✅ | ✅ | ✅ | Includes v1/v2 multi-delete test coverage in s3ite. |
| Objects | CopyObject | ✅ | ✅ | ✅ | ✅ | Same-bucket, cross-bucket, versioned, metadata-copy, and metadata-replace behavior. |
| Objects | CopyObject from access point ARN | ❌ | ✅ | ❌ | ❌ | s3ite explicitly returns `NotImplemented` for access point copy sources. |
| Objects | Range GET | ✅ | ✅ | ✅ | ✅ | Includes leading/trailing/invalid/empty-object range cases. |
| Objects | Completed multipart part reads with `partNumber` | ✅ | ✅ | ✅ | ✅ | s3ite supports `GetObject` and `HeadObject` part-number reads. |
| Objects | User metadata | ✅ | ✅ | ✅ | ✅ | s3ite stores and returns user metadata. |
| Objects | RFC2047/unicode metadata decoding | ✅ | ✅ | ✅ | ✅ | Covered by s3ite metadata conformance tests. |
| Objects | Cache-Control object header | ✅ | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Expires object header | ✅ | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Content-Type object header | ✅ | ✅ | ✅ | ✅ | Stored and returned by s3ite. |
| Objects | Content-Encoding object header | ✅ | ✅ | ✅ | ✅ | s3ite normalizes AWS chunked encoding. |
| Objects | Response header overrides on GET | ✅ | ✅ | ✅ | ✅ | Covered locally by raw response-header tests. |
| Objects | Request ID/error header compatibility | ✅ | ✅ | ✅ | ✅ | s3ite emits request IDs on the tested error paths. |
| Objects | GetObjectTorrent | ✅ | ✅ | ❌ | ❌ | s3ite reports torrent metadata as unavailable; the local s3-tests scope marks it out of scope. |
| Listings | ListObjects v1 | ✅ | ✅ | ✅ | ✅ | Includes prefix, delimiter, marker, max-keys, encoding, and owner behavior. |
| Listings | ListObjectsV2 | ✅ | ✅ | ✅ | ✅ | Includes continuation-token and start-after behavior. |
| Listings | Prefix filtering | ✅ | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | Delimiter/common-prefix grouping | ✅ | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | URL encoding in list responses | ✅ | ✅ | ✅ | ✅ | v1 and v2. |
| Listings | Unreadable/special key listing edge cases | ✅ | ✅ | ✅ | ✅ | s3ite covers special prefixes and key names. |
| Listings | FetchOwner in ListObjectsV2 | ✅ | ✅ | ✅ | ✅ | s3ite returns the synthetic owner. |
| Listings | ListObjectVersions | ✅ | ✅ | ✅ | ✅ | Includes versions and delete markers. |
| Auth | SigV4 signed owner requests | ✅ | ✅ | ✅ | ✅ | s3ite supports a single configured owner identity. |
| Auth | Presigned GET URLs | ✅ | ✅ | ✅ | ✅ | Includes expiry and tamper rejection. |
| Auth | Presigned PUT URLs | ✅ | ✅ | ✅ | ✅ | Includes owner-equivalent ACL compatibility. |
| Auth | SigV2 legacy compatibility edges | ✅ | ❌ | ✅ | ✅ | s3ite keeps SigV2 edge behavior for clone-test compatibility. |
| Auth | Unsigned public/anonymous access | ✅ | ✅ | ✅ | ❌ | s3ite denies unsigned requests when auth is configured. |
| Auth | Bucket policy API and enforcement | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Auth | IAM user/group/role/account APIs | ✅ | ✅ | ❌ | ❌ | MinIO has IAM concepts, but not the AWS IAM API surface tested by `test_iam.py`. |
| Auth | STS session credentials and AssumeRole | ✅ | ✅ | ✅ | ❌ | Covered by Ceph `test_sts.py`; out of scope for s3ite. |
| Auth | Cross-account authorization | ✅ | ✅ | ✅ | ❌ | s3ite is single-owner. |
| ACLs | GetBucketAcl | ✅ | ✅ | ❌ | ✅ | s3ite returns a synthetic owner `FULL_CONTROL` ACL. |
| ACLs | GetObjectAcl | ✅ | ✅ | ❌ | ✅ | s3ite returns a synthetic owner `FULL_CONTROL` ACL. |
| ACLs | Owner-equivalent canned ACL writes | ✅ | ✅ | ❌ | ✅ | s3ite accepts `private`/owner-equivalent writes as no-ops. |
| ACLs | Public canned ACL grants | ✅ | ✅ | ❌ | ❌ | s3ite rejects public ACLs. MinIO documents object ACL APIs as unsupported. |
| ACLs | Explicit grant headers/XML ACL bodies | ✅ | ✅ | ❌ | ❌ | s3ite rejects grant headers and ACL XML bodies. |
| ACLs | Object ownership controls | ✅ | ✅ | ❌ | ❌ | MinIO documents bucket ownership controls as unsupported. |
| Versioning | GetBucketVersioning | ✅ | ✅ | ✅ | ✅ | s3ite supports enabled/suspended/unset states. |
| Versioning | PutBucketVersioning enabled/suspended | ✅ | ✅ | ✅ | ✅ | s3ite supports enabled and suspended. |
| Versioning | Null version behavior while suspended | ✅ | ✅ | ✅ | ✅ | Covered by s3ite versioning tests. |
| Versioning | Version-specific GetObject/HeadObject | ✅ | ✅ | ✅ | ✅ | s3ite covers versioned reads and heads. |
| Versioning | Version-specific CopyObject | ✅ | ✅ | ✅ | ✅ | s3ite covers copy by source version. |
| Versioning | Version-specific DeleteObject | ✅ | ✅ | ✅ | ✅ | s3ite covers deleting explicit versions. |
| Versioning | Delete markers | ✅ | ✅ | ✅ | ✅ | Includes `x-amz-delete-marker` behavior. |
| Versioning | Multi-delete with version IDs | ✅ | ✅ | ✅ | ✅ | Covered locally, including delete-marker creation. |
| Versioning | ListObjectVersions pagination/markers | ✅ | ✅ | ✅ | ✅ | s3ite covers key/version markers. |
| Versioning | MFA Delete | ❌ | ✅ | ❌ | ❌ | s3ite rejects MFA Delete configuration. |
| Multipart | CreateMultipartUpload | ✅ | ✅ | ✅ | ✅ | Basic multipart initiation. |
| Multipart | UploadPart | ✅ | ✅ | ✅ | ✅ | Includes size and part-number validation. |
| Multipart | CompleteMultipartUpload | ✅ | ✅ | ✅ | ✅ | Includes ordering, ETag, and missing/incorrect part validation. |
| Multipart | AbortMultipartUpload | ✅ | ✅ | ✅ | ✅ | Includes missing upload behavior. |
| Multipart | ListMultipartUploads | ✅ | ✅ | ✅ | ✅ | s3ite supports prefix, delimiter, pagination, and owner fields. |
| Multipart | ListParts | ✅ | ✅ | ✅ | ✅ | s3ite supports pagination and ETags. |
| Multipart | UploadPartCopy | ✅ | ✅ | ✅ | ✅ | Includes ranges, conditions, special names, and versions. |
| Multipart | Multipart idempotent complete retry | ✅ | ✅ | ✅ | ✅ | s3ite records completion results for retries. |
| Multipart | Multipart re-upload part replacement | ✅ | ✅ | ✅ | ✅ | Covered by s3ite checksum and multipart tests. |
| Multipart | AbortIncompleteMultipartUpload lifecycle action | ✅ | ✅ | ❌ | ❌ | MinIO documents this lifecycle action as unsupported. |
| Checksums | Content-MD5 validation | ✅ | ✅ | ✅ | ✅ | Includes invalid short, malformed, bad, empty, and missing forms. |
| Checksums | PutObject SHA256 checksum | ✅ | ✅ | ✅ | ✅ | s3ite validates and stores the checksum. |
| Checksums | PutObject CRC64NVME checksum | ✅ | ✅ | ✅ | ✅ | s3ite validates and stores the checksum. |
| Checksums | Multipart SHA256 checksum | ✅ | ✅ | ✅ | ✅ | Includes composite checksum behavior. |
| Checksums | Multipart CRC32 checksum | ✅ | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart CRC32C checksum | ✅ | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart CRC64NVME checksum | ✅ | ✅ | ✅ | ✅ | Includes full-object checksum behavior. |
| Checksums | Multipart SHA1 checksum | ✅ | ✅ | ✅ | ✅ | Includes composite checksum behavior. |
| Checksums | GetObject checksum-mode response headers | ✅ | ✅ | ✅ | ✅ | s3ite returns stored checksums when requested. |
| Checksums | CopyObject checksum inheritance/defaulting | ✅ | ✅ | ✅ | ✅ | s3ite covers inherited and default checksum algorithms. |
| Checksums | Default CRC64NVME when checksum omitted | ❌ | ✅ | ❌ | ✅ | s3ite intentionally mirrors current AWS default behavior here. |
| Attributes | GetObjectAttributes basic metadata | ✅ | ✅ | ✅ | ✅ | ETag, size, storage class, checksum. |
| Attributes | GetObjectAttributes versioned objects | ✅ | ✅ | ✅ | ✅ | Covered by s3ite. |
| Attributes | GetObjectAttributes multipart parts | ✅ | ✅ | ✅ | ✅ | Includes part pagination. |
| Attributes | GetObjectAttributes multipart part checksums | ✅ | ✅ | ✅ | ✅ | Includes checksum attributes for parts. |
| Encryption | Explicit SSE-S3 PutObject (`AES256`) | ✅ | ✅ | ✅ | ✅ | s3ite encrypts stored bytes and returns plaintext on read. |
| Encryption | SSE-S3 HeadObject/GetObject response headers | ✅ | ✅ | ✅ | ✅ | s3ite returns `AES256` for encrypted objects. |
| Encryption | Bucket default SSE-S3 config | ✅ | ✅ | ✅ | ✅ | s3ite supports Put/Get/DeleteBucketEncryption for SSE-S3. |
| Encryption | SSE-S3 multipart upload | ✅ | ✅ | ✅ | ✅ | Covered by default bucket encryption and multipart tests. |
| Encryption | SSE-S3 CopyObject | ✅ | ✅ | ✅ | ✅ | s3ite covers plaintext/encrypted source and destination permutations. |
| Encryption | SSE-S3 UploadPartCopy | ✅ | ✅ | ✅ | ✅ | s3ite covers plaintext/encrypted source and destination permutations. |
| Encryption | SSE-KMS object encryption | ✅ | ✅ | ✅ | ❌ | s3ite rejects KMS headers as unsupported. |
| Encryption | Bucket default SSE-KMS | ✅ | ✅ | ✅ | ❌ | s3ite only supports bucket-default SSE-S3. |
| Encryption | SSE-C object encryption | ✅ | ✅ | ✅ | ❌ | s3ite rejects SSE-C headers as unsupported. |
| Encryption | SSE-C multipart | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Encryption | S3 Bucket Keys | ❌ | ✅ | ❌ | ❌ | s3ite rejects bucket-key settings. |
| Encryption | DSSE-KMS | ❌ | ✅ | ❌ | ❌ | Not implemented by s3ite. |
| Encryption | Encryption conflict validation | ✅ | ✅ | ✅ | ✅ | s3ite rejects mixed SSE-C/SSE-KMS/SSE-S3 request shapes. |
| Tagging | Bucket tagging API | ✅ | ✅ | ✅ | ✅ | Covers Get/Put/DeleteBucketTagging. |
| Tagging | Object tagging API | ✅ | ✅ | ✅ | ✅ | Covers Get/Put/DeleteObjectTagging with version-specific tags. |
| Tagging | Object tags on PutObject | ✅ | ✅ | ✅ | ✅ | Covers PutObject, CopyObject tag directives, and multipart upload tags. |
| Tagging | Object tags on POST Object | ✅ | ✅ | ✅ | ✅ | POST Object forwards `x-amz-tagging` through the normal object write path. |
| Tagging | Tag-based bucket policy conditions | ✅ | ✅ | ✅ | ❌ | Requires bucket policy support. |
| Lifecycle | Put/Get/DeleteBucketLifecycle | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | Current object expiration | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | Noncurrent version expiration | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | Delete-marker expiration | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | Lifecycle filters by prefix/tag/size | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | Storage-class transitions | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Lifecycle | RestoreObject from archive/tier | ✅ | ✅ | ✅ | ❌ | MinIO lists `RestoreObject`; s3ite has no restore/tiering. |
| Lifecycle | Restore status in list outputs | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| CORS | Bucket CORS API | ✅ | ✅ | ❌ | ✅ | s3ite stores per-bucket CORS rules in each bucket DB; MinIO documents bucket CORS APIs as unsupported because CORS is enabled by default. |
| CORS | CORS preflight behavior | ✅ | ✅ | ✅ | ✅ | s3ite applies bucket CORS first and falls back to configurable server-level CORS when no bucket config exists. |
| POST | POST Object form uploads | ✅ | ✅ | ✅ | ✅ | Browser form uploads reuse the normal object write path. |
| POST | POST Object policy validation | ✅ | ✅ | ✅ | ✅ | SigV4 POST policy validation is handled by `s3s`. |
| POST | POST Object success redirects/status codes | ✅ | ✅ | ✅ | ✅ | Covers default 204, 200/201 statuses, and 303 redirects. |
| Object Lock | Bucket Object Lock configuration | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Object Lock | Object retention governance/compliance | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Object Lock | Object legal hold | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Object Lock | Delete blocked by retention/legal hold | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Object Lock | Retention metadata on HeadObject/GetObject | ✅ | ✅ | ✅ | ❌ | Out of scope for s3ite. |
| Policy/Security | PublicAccessBlock APIs | ✅ | ✅ | ❌ | ❌ | MinIO documents these APIs as unsupported. |
| Policy/Security | Bucket policy status API | ✅ | ✅ | ✅ | ❌ | Requires bucket policy support. |
| Policy/Security | Requester Pays / request payment | ✅ | ✅ | ❌ | ❌ | MinIO documents request-payment APIs as unsupported. |
| Notifications | Bucket notification configuration | ✅ | ✅ | ✅ | ❌ | MinIO documents bucket notifications; s3ite does not implement them. |
| Notifications | SNS-compatible notification tests | ✅ | ✅ | ✅ | ❌ | Covered by Ceph `test_sns.py`; out of scope for s3ite. |
| Replication | Bucket replication configuration | ❌ | ✅ | ✅ | ❌ | MinIO documents bucket replication; not covered by local Ceph test audit. |
| Logging | S3 server access logging | ✅ | ✅ | ❌ | ❌ | MinIO documents bucket logging APIs as unsupported; s3ite has application logs only. |
| Website | Static website bucket configuration | ❌ | ✅ | ❌ | ❌ | MinIO documents website APIs as unsupported. |
| Inventory | Bucket inventory configuration | ❌ | ✅ | ❌ | ❌ | MinIO documents inventory APIs as unsupported. |
| Analytics | Bucket analytics configuration | ❌ | ✅ | ❌ | ❌ | MinIO documents analytics APIs as unsupported. |
| Metrics | Bucket metrics configuration | ❌ | ✅ | ❌ | ❌ | MinIO documents metrics APIs as unsupported. |
| Accelerate | Bucket accelerate configuration | ❌ | ✅ | ❌ | ❌ | MinIO documents accelerate APIs as unsupported. |
| Storage class | STANDARD storage-class reporting | ✅ | ✅ | ✅ | ✅ | s3ite reports `STANDARD` in list/object-attribute shapes. |
| Storage class | REDUCED_REDUNDANCY PutObject validation | ❌ | ✅ | ✅ | ✅ | s3ite accepts this request value but does not implement tiered placement. |
| Storage class | Multiple durable storage classes | ✅ | ✅ | ✅ | ❌ | s3ite has one SQLite-backed placement mode. |
| Storage class | Glacier/Deep Archive restore workflow | ✅ | ✅ | ✅ | ❌ | Requires lifecycle/tiering/restore. |
| Advanced | SelectObjectContent / S3 Select | ✅ | ✅ | ✅ | ❌ | Covered by Ceph `test_s3select.py`; out of scope for s3ite. |
| Advanced | S3 Control account public access block | ✅ | ✅ | ❌ | ❌ | Covered by Ceph `test_s3control.py`; not a s3ite surface. |
| Advanced | Access Points | ❌ | ✅ | ❌ | ❌ | s3ite rejects access point copy sources and has no access point routing. |
| Advanced | Multi-Region Access Points | ❌ | ✅ | ❌ | ❌ | AWS-only managed service feature. |
| Advanced | S3 on Outposts | ❌ | ✅ | ❌ | ❌ | AWS-only managed service feature. |
| Advanced | Directory buckets / S3 Express One Zone | ❌ | ✅ | ❌ | ❌ | s3ite has ordinary bucket databases only. |
| Advanced | S3 Object Lambda transforms | ❌ | ✅ | ✅ | ❌ | AWS marks S3 Object Lambda as existing-customer/APN-limited after 2025-11-07; MinIO has Object Lambda transform docs; s3ite does not implement transforms. |
| Advanced | Object Lambda AWS control APIs | ❌ | ✅ | ❌ | ❌ | AWS managed control-plane feature. |
| Advanced | S3 Tables APIs | ❌ | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Tables separately; not s3ite. |
| Advanced | S3 Vectors APIs | ❌ | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Vectors separately; not s3ite. |
| Advanced | S3 Files APIs | ❌ | ✅ | ❌ | ❌ | AWS S3 API reference exposes S3 Files separately; not s3ite. |

## s3ite Gaps To Track

The largest compatibility gaps relative to AWS/MinIO and `ceph/s3-tests` are:

- Auth and authorization: bucket policies, public/anonymous access, IAM, STS,
  cross-account behavior, and full ACL grant enforcement.
- Object-management APIs: lifecycle, restore/tiering, Object Lock,
  PublicAccessBlock, notifications, logging, website hosting, inventory,
  analytics, metrics, and request payment.
- Encryption variants: SSE-KMS, SSE-C, DSSE-KMS, and S3 Bucket Keys.
- AWS managed/control-plane features: S3 Control, access points,
  Multi-Region Access Points, S3 Express/directory buckets, S3 on Outposts,
  Object Lambda control APIs, S3 Tables, S3 Vectors, and S3 Files.

s3ite's implemented surface is concentrated on the data path: bucket lifecycle,
object CRUD, listings, versioning, multipart, checksums, owner-only auth/ACL
compatibility, selected raw HTTP edge cases, and SSE-S3.
