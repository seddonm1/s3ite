# DO-178-Inspired Testing Plan

## Summary

s3ite uses SQLite as both an implementation dependency and a quality model. SQLite's testing
process is influenced by DO-178B, but SQLite deliberately keeps the parts that improve quality
without adopting full certification ceremony. s3ite should follow the same spirit: requirements
based tests, lightweight traceability, structural coverage as a test-quality signal, regression
discipline, fault injection, and tests against the delivered runtime configuration.

This is not a DO-178B or DO-178C certification plan. No claim is made that s3ite satisfies any
Design Assurance Level, tool qualification process, MC/DC objective, or certification artifact
set. The goal is a practical, low-ceremony test strategy for an S3-compatible SQLite service.

References:

- SQLite Quality Management Plan: https://sqlite.org/qmplan.html
- How SQLite Is Tested: https://www.sqlite.org/testing.html
- SQLite TH3 and DO-178B background: https://sqlite.org/th3.html

## Testing Philosophy

Tests should prove externally meaningful behavior first, then implementation invariants. A bug or
conformance gap is not fixed until a named regression test demonstrates the old failure and the new
behavior. Tests should be cheap enough to run often, but broad enough that they exercise the same
configuration paths users run: the binary, YAML config loading, auth middleware, compatibility
layers, Tower service stack, SQLite worker pools, and file-backed bucket databases.

The useful DO-178B/C lessons for s3ite are:

- Every supported behavior should be traceable to one or more tests.
- Tests should include normal, boundary, and invalid-input cases.
- Coverage reports are meta-tests of the test suite, not proof that the product is correct.
- Important decisions and conditionals should have targeted tests that force both meaningful
  outcomes.
- Fault and resource-failure behavior deserves explicit tests instead of optimistic assumptions.
- The delivered runtime shape must be tested, not only internal helper functions.

## Traceability Model

Each supported behavior gets a lightweight requirement ID. Requirement IDs are stable labels used in
test names, comments, and future issue/PR descriptions. They are not a separate formal requirements
database.

Initial requirement groups:

| Group | Area |
| --- | --- |
| `OBJ` | Object CRUD, object headers, byte ranges, `partNumber`, read/write conditionals |
| `LIST` | `ListObjects`, `ListObjectsV2`, `ListObjectVersions`, prefixes, delimiters, markers |
| `MPU` | Multipart create, upload, list, complete, copy, abort, idempotency |
| `VER` | Bucket versioning, null versions, delete markers, exact version operations |
| `CHK` | Checksums, default `CRC64NVME`, multipart checksums, `GetObjectAttributes` |
| `AUTH` | Signed requests, unsigned denial, presigned URLs, owner-only compatibility |
| `DB` | Schema constraints, SQLite error mapping, worker queues, BLOB streaming |
| `ROBUST` | Malformed input, raw HTTP edge cases, read-only mode, queue/close behavior |

Requirement IDs should use the form `GROUP-short-behavior`, for example:

- `OBJ-range-suffix`
- `LIST-v2-token`
- `MPU-complete-duplicate-part`
- `AUTH-presigned-expiry`
- `DB-blob-reader-timeout`

Rust conformance tests that port Ceph behavior should include a source comment:

```rust
// Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_unreadable
```

Unsupported behavior must be explicit. Use an ignored test, an unsupported-surface list, or a
documented Ceph filter entry with the reason, rather than silently omitting the behavior.

## Test Layers

### Colocated Unit Tests

Colocated unit tests live beside the implementation in `src/s3/*` and `src/sqlite/*`.

Use them for pure logic and small storage invariants:

- S3 parsing, validation, range decisions, checksum policy selection, continuation tokens, and
  owner-only compatibility.
- SQLite row conversions, schema constraints, version state transitions, keyset pagination,
  multipart completion validation, and maintenance helpers.
- In-memory SQLite is appropriate for storage unit tests when the behavior does not depend on
  filename, WAL, multi-connection reader pools, or BLOB streaming through `BucketConnection`.

### Real-Server Integration Tests

Real conformance tests live in `tests/conformance_*.rs` and start the actual `s3ite` binary. These
tests prove behavior across the delivered stack:

- command-line or YAML config loading
- auth and presigned request handling
- `compat.rs` middleware behavior
- host/path routing
- HTTP serialization and XML parsing through `s3s`
- Tower layers, CORS behavior, and response headers
- file-backed SQLite bucket databases, WAL sidecars, worker pools, and BLOB streaming

Server integration tests must use temporary file-backed buckets, not in-memory SQLite. They should
coordinate with `serial_test`, use unique temp roots and bucket prefixes, and preserve server logs on
failure.

### Raw HTTP Conformance Tests

Some conformance cases cannot be expressed through the AWS SDK. Raw HTTP helpers should cover:

- unsigned requests
- malformed or expired presigned URLs
- bad signatures and malformed auth headers
- invalid XML bodies
- unusual method/header combinations
- CORS/preflight requests when that surface is being tested

### Ceph Test Port

The Rust conformance suite should port every Ceph `s3-tests` case that works against s3ite's
implemented surface. "Works" means the test either already passes against s3ite or exercises an S3
surface that s3ite claims to support and should therefore be made to pass. Unsupported product
surfaces are not silently skipped; they are tracked with an explicit reason.

Maintain a porting manifest in the test documentation or in a generated test-support table with one
row per upstream Ceph selector:

- `ported`: implemented as a Rust conformance test.
- `pending`: supported behavior that still needs a Rust port.
- `unsupported`: outside s3ite's current scope, with a reason.
- `rgw-specific`: Ceph/RGW-specific behavior that should not define s3ite's contract.
- `blocked`: cannot be represented yet because the Rust harness lacks a raw HTTP/helper capability.

Each ported test must include a source comment:

```rust
// Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_unreadable
```

The target is that `pending` reaches zero for the supported surface. Ceph `s3-tests` remains the
external oracle for discovering new cases and checking that the Rust port has not drifted from
upstream behavior.

## Coverage Strategy

Line and branch coverage should be used as a meta-test of the test suite. A coverage run answers
"did our tests execute this code?" It does not prove correctness.

s3ite should not claim full MC/DC. Instead:

- Add targeted decision tests for complex S3 conditionals, checksum precedence, versioning state,
  delete marker behavior, multipart completion validation, and SQLite error mapping.
- Treat untested branches in core behavior as prompts for either a test or dead-code removal.
- Run coverage periodically on the Rust suite and track trends for `src/s3`, `src/sqlite`, and
  `src/database.rs`.
- Keep fuzz and malformed-input tests separate from coverage targets; robustness and structural
  coverage improve different failure modes.

## Regression Rule

Every fixed bug, resource leak, panic, data-loss risk, or conformance gap must add a named
regression test. The test should be placed at the narrowest useful layer:

- Pure logic bug: colocated unit test.
- Storage invariant or transaction bug: SQLite unit test, preferably with in-memory SQLite unless
  the bug depends on file-backed behavior.
- Wire/protocol/auth bug: real-server integration test.
- Malformed request bug: raw HTTP integration test.
- Ceph conformance bug: Rust port/regression test plus focused `scripts/run-s3-tests.sh` selector.

Regression tests should name the behavior rather than the incident. Comments may reference the
upstream Ceph selector or issue number when useful.

## Acceptance Gates

Default validation before merging behavior changes:

```bash
cargo fmt --all --check
cargo check --no-default-features --lib
cargo clippy --features binary --all-targets
cargo test --features binary
```

For changes touching S3 behavior, also run focused Ceph selectors through:

```bash
scripts/run-s3-tests.sh <pytest-selector-or-args>
```

For broader confidence, periodically run the Ceph working set that corresponds to the Rust port
manifest. Any selector that passes and is in scope should eventually be ported. Any selector that
fails should be classified as a product bug, unsupported surface, RGW-specific behavior, or harness
gap.

## Defaults For Future Test Work

- Split the current monolithic `tests/test.rs` into focused integration files instead of adding more
  tests to it.
- Prefer colocated unit tests for new helper logic.
- Prefer real-server tests for Ceph ports and conformance claims.
- Use `serial_test` for real-server tests that share process-level resources or expensive setup.
- Keep test data small except where a test is specifically about BLOB streaming or chunk
  boundaries.
- Keep unsupported behavior visible through the Ceph port manifest, explicit ignored tests,
  documented filters, or a tracked unsupported-surface list.
