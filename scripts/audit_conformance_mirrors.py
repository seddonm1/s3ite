#!/usr/bin/env python3
"""Audit Rust conformance tests for Ceph s3-tests mirror hygiene."""

from __future__ import annotations

import re
import sys
from pathlib import Path


TEST_ATTR = re.compile(r"^\s*#\[(?:tokio::test|test)\]")
TEST_FN = re.compile(r"\b(?:async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(")
SOURCE_COMMENT = re.compile(
    r"(Mirrors|Adapted from) s3tests/functional/test_s3\.py::([^\s:]+)(?::\s*(.+))?"
)
SUPPORTED_COPY_ENCRYPTIONS = ("unencrypted", "sse-s3")
SUPPORTED_COPY_SIZES = (1, 1024, 1024 * 1024, 8 * 1024 * 1024)
SUPPORTED_COPY_PART_SIZE = 8 * 1024 * 1024
REQUIRED_SINGLE_TEST_PORTS = (
    "test_versioning_obj_plain_null_version_overwrite",
    "test_get_object_torrent",
    "test_object_set_get_unicode_metadata",
)


SourceRecord = tuple[str, str, Path, int, str]


def is_parametrized_mirror(mirror: str) -> bool:
    return "[" in mirror and mirror.endswith("]")


def required_mirror_comments() -> set[str]:
    required = set(REQUIRED_SINGLE_TEST_PORTS)
    for source in SUPPORTED_COPY_ENCRYPTIONS:
        for destination in SUPPORTED_COPY_ENCRYPTIONS:
            for size in SUPPORTED_COPY_SIZES:
                required.add(
                    f"test_copy_enc[{source}-{destination}-STANDARD-STANDARD-{size}]"
                )
            required.add(
                f"test_copy_part_enc[{source}-{destination}-STANDARD-STANDARD-{SUPPORTED_COPY_PART_SIZE}]"
            )
    return required


def audit_file(path: Path, display_path: Path) -> tuple[list[str], list[SourceRecord]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    errors: list[str] = []
    source_records: list[SourceRecord] = []
    index = 0
    while index < len(lines):
        if not TEST_ATTR.match(lines[index]):
            index += 1
            continue

        fn_name = None
        fn_line = None
        for cursor in range(index + 1, min(index + 12, len(lines))):
            match = TEST_FN.search(lines[cursor])
            if match:
                fn_name = match.group(1)
                fn_line = cursor + 1
                break
        if fn_name is None or fn_line is None:
            errors.append(f"{display_path}:{index + 1}: test attribute without test function")
            index += 1
            continue

        sources: list[tuple[str, str, str | None, int]] = []
        for cursor in range(fn_line, min(fn_line + 12, len(lines))):
            match = SOURCE_COMMENT.search(lines[cursor])
            if match:
                sources.append(
                    (match.group(1), match.group(2), match.group(3), cursor + 1)
                )
            if cursor > fn_line and TEST_FN.search(lines[cursor]):
                break

        if not sources:
            if not fn_name.startswith("s3ite_"):
                errors.append(
                    f"{display_path}:{fn_line}: {fn_name} has no Ceph source comment or s3ite_ prefix"
                )
        elif len(sources) > 1:
            errors.append(
                f"{display_path}:{fn_line}: {fn_name} has multiple Ceph source comments"
            )
        else:
            kind, source, reason, comment_line = sources[0]
            source_records.append((kind, source, display_path, fn_line, fn_name))
            if kind == "Adapted from" and not reason:
                errors.append(
                    f"{display_path}:{comment_line}: {fn_name} adapted source comment needs a reason"
                )
            if kind == "Mirrors" and not is_parametrized_mirror(source) and source != fn_name:
                errors.append(
                    f"{display_path}:{fn_line}: {fn_name} mirrors {source}; non-parametrized mirrors must use the Ceph name"
                )

        index = fn_line

    return errors, source_records


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    errors: list[str] = []
    source_records: list[SourceRecord] = []
    for path in sorted((repo / "tests").glob("conformance_*.rs")):
        file_errors, file_sources = audit_file(path, path.relative_to(repo))
        errors.extend(file_errors)
        source_records.extend(file_sources)

    seen_sources: dict[str, SourceRecord] = {}
    for kind, source, path, line, fn_name in source_records:
        if source in seen_sources:
            first_kind, _, first_path, first_line, first_fn_name = seen_sources[source]
            errors.append(
                f"{path}:{line}: {fn_name} duplicates Ceph source {source}; first seen as {first_kind} at {first_path}:{first_line} in {first_fn_name}"
            )
        else:
            seen_sources[source] = (kind, source, path, line, fn_name)

    sourced = {source for _, source, _, _, _ in source_records}
    for missing in sorted(required_mirror_comments() - sourced):
        errors.append(f"tests: missing required Ceph source port {missing}")

    object_lock = repo / "tests" / "conformance_object_lock.rs"
    object_lock_count = len(
        re.findall(
            r"^\s*async\s+fn\s+test_object_lock",
            object_lock.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
    )
    if object_lock_count != 39:
        errors.append(
            f"tests/conformance_object_lock.rs: expected 39 Object Lock ports, found {object_lock_count}"
        )

    if errors:
        print("Conformance mirror audit failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    print("Conformance mirror audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
