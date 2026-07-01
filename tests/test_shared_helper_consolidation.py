"""Regression tests for shared DB and chain helper consolidation."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

CONSOLIDATED_HELPERS = {
    "_placeholder": "adapters/base.py",
    "_is_sqlite": "adapters/base.py",
    "_is_sqlite_connection": "adapters/base.py",
    "_columns": "adapters/base.py",
    "_get_columns": "adapters/base.py",
    "_ensure_api_usage_table": "adapters/base.py",
    "_cursor_rows_to_dicts": "chains/utils.py",
    "_extract_json_text": "chains/utils.py",
    "_guard_context": "chains/utils.py",
    "_acquire_connection": "chains/utils.py",
    "_normalize_cik": "utils/identifiers.py",
}

SCANNED_DIRS = ("adapters", "api", "chains", "etl", "llm", "scripts")


def _python_files() -> list[Path]:
    files: list[Path] = []
    for dirname in SCANNED_DIRS:
        files.extend(sorted((REPO_ROOT / dirname).glob("**/*.py")))
    return files


def test_consolidated_helpers_are_not_redefined_locally() -> None:
    duplicates: list[str] = []
    for path in _python_files():
        relative = path.relative_to(REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                expected_home = CONSOLIDATED_HELPERS.get(node.name)
                if expected_home is not None and relative != expected_home:
                    duplicates.append(f"{relative}:{node.lineno}:{node.name}")

    assert duplicates == []
