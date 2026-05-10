#!/usr/bin/env python3
"""Reject unaudited SQLite-only SQL on Postgres-capable code paths."""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SQLITE_ONLY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("AUTOINCREMENT", re.compile(r"\bAUTOINCREMENT\b", re.IGNORECASE)),
    ("INSERT OR IGNORE", re.compile(r"\bINSERT\s+OR\s+IGNORE\b", re.IGNORECASE)),
    ("PRAGMA table_info", re.compile(r"\bPRAGMA\s+table_info\b", re.IGNORECASE)),
    ("PRAGMA table_xinfo", re.compile(r"\bPRAGMA\s+table_xinfo\b", re.IGNORECASE)),
    (
        "SQLITE ? placeholder",
        re.compile(
            r"\b(?:execute|executemany)\s*\([^#\n]*"
            r"(?:SELECT|INSERT|UPDATE|DELETE|VALUES|WHERE)[^#\n]*\?",
            re.IGNORECASE,
        ),
    ),
    ("legacy daily_diff table", re.compile(r"\bdaily_diff\b", re.IGNORECASE)),
    ("legacy d.change column", re.compile(r"\bd\.change\b", re.IGNORECASE)),
)

DEFAULT_SCAN_ROOTS = (
    "adapters",
    "etl",
    "chains",
    "alerts",
    "api",
    "ui",
    "llm",
    "scripts",
    "embeddings.py",
    "diff_holdings.py",
)

AUDITED_SQLITE_ONLY_ALLOWLIST: dict[str, set[str]] = {
    "etl/daily_diff_flow.py": {"AUTOINCREMENT"},
    "etl/digest_flow.py": {"PRAGMA table_xinfo"},
    "etl/edgar_flow.py": {"PRAGMA table_xinfo"},
}

AUDIT_REPORT_PATH = Path("docs/reports/dialect_portability_audit.md")


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    token: str
    text: str


def _relative_path(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _python_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(
                child
                for child in path.rglob("*.py")
                if not any(part in {"tests", "__pycache__", ".venv"} for part in child.parts)
                and child.name != "check_dialect_portability.py"
            )
        elif path.suffix == ".py":
            files.append(path)
    return sorted(set(files))


def _imports_connect_db(source: str) -> bool:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return "connect_db(" in source
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == "adapters.base" and any(
                alias.name == "connect_db" for alias in node.names
            ):
                return True
        elif isinstance(node, ast.FunctionDef) and node.name == "connect_db":
            return True
    return "connect_db(" in source


def scan(
    paths: list[Path],
    *,
    repo_root: Path,
    allowlist: dict[str, set[str]],
    scan_all: bool = False,
) -> list[Finding]:
    findings: list[Finding] = []
    for path in _python_files(paths):
        source = path.read_text(encoding="utf-8")
        if not scan_all and not _imports_connect_db(source):
            continue
        rel_path = _relative_path(path, repo_root)
        allowed_tokens = allowlist.get(rel_path, set())
        for lineno, line in enumerate(source.splitlines(), start=1):
            for token, pattern in SQLITE_ONLY_PATTERNS:
                if pattern.search(line) and token not in allowed_tokens:
                    findings.append(Finding(rel_path, lineno, token, line.strip()))
    return findings


def documented_audit_paths(audit_report: Path) -> set[str]:
    """Return audited module paths listed in the disposition table."""
    if not audit_report.exists():
        return set()

    paths: set[str] = set()
    for line in audit_report.read_text(encoding="utf-8").splitlines():
        match = re.search(r"\|\s*`([^`]+\.py)`\s*\|", line)
        if match:
            paths.add(match.group(1))
    return paths


def allowlist_paths_missing_from_audit(
    *, repo_root: Path, allowlist: dict[str, set[str]]
) -> list[str]:
    audited_paths = documented_audit_paths(repo_root / AUDIT_REPORT_PATH)
    missing = sorted(path for path in allowlist if path not in audited_paths)
    return missing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to scan. Defaults to production DB-facing surfaces.",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root used for allowlist-relative paths.",
    )
    parser.add_argument(
        "--no-allowlist",
        action="store_true",
        help="Ignore the audited allowlist; useful for tests.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    raw_paths = args.paths or list(DEFAULT_SCAN_ROOTS)
    paths = [(repo_root / raw_path).resolve() for raw_path in raw_paths]
    allowlist = {} if args.no_allowlist else AUDITED_SQLITE_ONLY_ALLOWLIST
    if allowlist:
        missing_audit_entries = allowlist_paths_missing_from_audit(
            repo_root=repo_root,
            allowlist=allowlist,
        )
        if missing_audit_entries:
            for path in missing_audit_entries:
                print(
                    f"allowlist entry missing from audit report: {path}",
                    file=sys.stderr,
                )
            print(
                f"Update {AUDIT_REPORT_PATH.as_posix()} disposition table before allowlisting new paths.",
                file=sys.stderr,
            )
            return 1

    findings = scan(
        paths,
        repo_root=repo_root,
        allowlist=allowlist,
        scan_all=bool(args.paths),
    )
    if findings:
        for finding in findings:
            print(
                f"{finding.path}:{finding.line}: unaudited {finding.token}: {finding.text}",
                file=sys.stderr,
            )
        print(
            "Dialect portability gate failed; classify this path in "
            "docs/reports/dialect_portability_audit.md before allowing it.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
