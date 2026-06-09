#!/usr/bin/env python3
"""Audit Rust conformance tests for Ceph s3-tests mirror hygiene."""

from __future__ import annotations

import re
import sys
from pathlib import Path


TEST_ATTR = re.compile(r"^\s*#\[(?:tokio::test|test)\]")
TEST_FN = re.compile(r"\b(?:async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(")
MIRROR = re.compile(r"Mirrors s3tests/functional/test_s3\.py::([^\s]+)")
SUPPORTED_COPY_ENCRYPTIONS = ("unencrypted", "sse-s3")
SUPPORTED_COPY_SIZES = (1, 1024, 1024 * 1024, 8 * 1024 * 1024)
SUPPORTED_COPY_PART_SIZE = 8 * 1024 * 1024
REQUIRED_SINGLE_TEST_PORTS = (
    "test_versioning_obj_plain_null_version_overwrite",
    "test_get_object_torrent",
    "test_object_set_get_unicode_metadata",
)


MirrorRecord = tuple[str, Path, int, str]


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


def audit_file(path: Path, display_path: Path) -> tuple[list[str], list[MirrorRecord]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    errors: list[str] = []
    mirror_records: list[MirrorRecord] = []
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

        mirrors: list[str] = []
        for cursor in range(fn_line, min(fn_line + 12, len(lines))):
            match = MIRROR.search(lines[cursor])
            if match:
                mirrors.append(match.group(1))
            if cursor > fn_line and TEST_FN.search(lines[cursor]):
                break

        if not mirrors:
            if not fn_name.startswith("s3ite_"):
                errors.append(
                    f"{display_path}:{fn_line}: {fn_name} has no Ceph mirror comment or s3ite_ prefix"
                )
        elif len(mirrors) > 1:
            errors.append(
                f"{display_path}:{fn_line}: {fn_name} has multiple Ceph mirror comments"
            )
        else:
            mirror = mirrors[0]
            mirror_records.append((mirror, display_path, fn_line, fn_name))
            if not is_parametrized_mirror(mirror) and mirror != fn_name:
                errors.append(
                    f"{display_path}:{fn_line}: {fn_name} mirrors {mirror}; non-parametrized mirrors must use the Ceph name"
                )

        index = fn_line

    return errors, mirror_records


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    errors: list[str] = []
    mirror_records: list[MirrorRecord] = []
    for path in sorted((repo / "tests").glob("conformance_*.rs")):
        file_errors, file_mirrors = audit_file(path, path.relative_to(repo))
        errors.extend(file_errors)
        mirror_records.extend(file_mirrors)

    seen_mirrors: dict[str, MirrorRecord] = {}
    for mirror, path, line, fn_name in mirror_records:
        if mirror in seen_mirrors:
            _, first_path, first_line, first_fn_name = seen_mirrors[mirror]
            errors.append(
                f"{path}:{line}: {fn_name} duplicates Ceph mirror {mirror}; first seen at {first_path}:{first_line} in {first_fn_name}"
            )
        else:
            seen_mirrors[mirror] = (mirror, path, line, fn_name)

    mirrored = {mirror for mirror, _, _, _ in mirror_records}
    for missing in sorted(required_mirror_comments() - mirrored):
        errors.append(f"tests: missing required Ceph mirror port {missing}")

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
