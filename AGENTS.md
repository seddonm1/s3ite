# Agent Invariants

This repository is a Rust S3-compatible service backed by SQLite. Keep future
changes boring in the best way: preserve layer boundaries, document behavior as
it moves, and let CI enforce quality instead of papering over failures.

## Layer Boundaries

- Keep S3 protocol concerns in `src/s3`.
- Keep SQLite and storage concerns in `src/sqlite` and `src/database.rs`.
- Do not introduce `s3s` DTOs, HTTP request/response types, or S3 response
  construction into SQLite code.
- Prefer storage-neutral types at the S3/SQLite boundary. If a shared type
  currently lives in the wrong layer, avoid widening that leak; move toward a
  neutral boundary type when a change naturally touches it.
- SQLite may expose crate-private storage summaries and pages. S3 maps those
  into protocol DTOs, owners, URL encoding, and response-specific shapes.

## Rust Documentation

- Every new Rust function should have a Rust doc comment.
- Use `///` for items and `//!` for modules.
- When modifying an existing undocumented function, add a concise Rust-style
  doc comment that explains what the function does and any important
  transactional, ordering, or boundary invariants.
- Keep comments useful. Prefer explaining behavior, constraints, or intent over
  restating the function name.

## SQL

- Keep SQL operation text inline with positional parameters unless the whole
  repository adopts a different convention.
- Large SQL statements need nearby documentation explaining the shape they
  return, their ordering and pagination rules, and any marker or grouping
  semantics they preserve.
- Do not mix named and positional SQL parameters casually. Consistency matters
  more than local cleverness here.

## Testing And CI

CI in `.github/workflows/rust.yml` runs:

- `cargo llvm-cov --all-targets --workspace --fail-under-lines 95 --fail-under-file-lines 95`
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --workspace -- -D warnings`

When behavior changes, add or adjust focused tests so the per-file coverage gate
stays healthy. Prefer focused tests first, then broader tests when behavior,
storage contracts, pagination, transactions, or S3 response shape move.

## Clippy

- Do not add production `#[allow(clippy::...)]` attributes unless the reason is
  unavoidable and documented next to the allow.
- Treat `too_many_lines`, `too_many_arguments`, and similar lints as design
  pressure first. Split responsibilities or introduce small coherent context
  types when that makes the code clearer.
- Test-only allows are acceptable when they support compact fixtures or test
  data setup, but they should not hide production shape problems.
