#!/usr/bin/env python3
"""Audit production code for transaction-only store boundaries."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"


CHECKS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "function accepts BucketConnection",
        re.compile(r"\b(?:async\s+)?fn\b[^{;]*&\s*BucketConnection"),
    ),
    (
        "function accepts raw rusqlite connection",
        re.compile(r"\b(?:async\s+)?fn\b[^{;]*&\s*mut\s+rusqlite::Connection"),
    ),
    (
        "function accepts raw Connection alias",
        re.compile(r"\b(?:async\s+)?fn\b[^{;]*&\s*Connection\b"),
    ),
    (
        "caller opens transaction manually",
        re.compile(r"\.transaction\(\)\?"),
    ),
    (
        "BucketConnection read/write closure exposes connection",
        re.compile(r"\.(?:read|write)\(\s*(?:move\s*)?\|\s*connection\s*\|"),
    ),
)

ALLOWLIST: tuple[tuple[str, str], ...] = (
    ("src/database.rs", "database worker owns raw SQLite connections"),
    ("src/sqlite/mod.rs", "SQLite schema preparation and maintenance use raw infrastructure"),
)


def production_text(path: Path) -> str:
    text = path.read_text()
    marker = "#[cfg(test)]"
    if marker in text:
        text = text[: text.index(marker)]
    return text


def is_allowlisted(path: Path) -> bool:
    display = path.relative_to(REPO).as_posix()
    return any(display == allowed for allowed, _reason in ALLOWLIST)


def audit_file(path: Path) -> list[str]:
    if is_allowlisted(path):
        return []

    errors: list[str] = []
    text = production_text(path)
    display = path.relative_to(REPO)
    for line_number, line in enumerate(text.splitlines(), start=1):
        for label, pattern in CHECKS:
            if pattern.search(line):
                errors.append(f"{display}:{line_number}: {label}: {line.strip()}")
    return errors


def main() -> int:
    errors: list[str] = []
    for path in sorted(SRC.rglob("*.rs")):
        errors.extend(audit_file(path))

    if errors:
        print("Transaction boundary audit failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    print("Transaction boundary audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
