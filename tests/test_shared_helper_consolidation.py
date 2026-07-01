"""Regression tests for shared DB and chain helper consolidation."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

CONSOLIDATED_HELPERS = {
    "_placeholder": "adapters/base.py",
    "_is_sqlite": "adapters/base.py",
    "_is_postgres": "adapters/base.py",
    "_is_sqlite_connection": "adapters/base.py",
    "_columns": "adapters/base.py",
    "_get_columns": "adapters/base.py",
    "_ensure_api_usage_table": "adapters/base.py",
    "get_placeholder": "adapters/base.py",
    "is_sqlite": "adapters/base.py",
    "is_postgres": "adapters/base.py",
    "table_exists": "adapters/base.py",
    "get_table_columns": "adapters/base.py",
    "manager_id_column": "adapters/base.py",
    "ensure_api_usage_schema": "adapters/base.py",
    "_cursor_rows_to_dicts": "chains/utils.py",
    "_extract_json_text": "chains/utils.py",
    "_guard_context": "chains/utils.py",
    "_acquire_connection": "chains/utils.py",
    "rows_to_dicts": "chains/utils.py",
    "extract_json_text": "chains/utils.py",
    "guard_context_values": "chains/utils.py",
    "acquire_connection": "chains/utils.py",
    "_normalize_cik": "utils/identifiers.py",
    "normalize_cik": "utils/identifiers.py",
}

SCANNED_DIRS = ("adapters", "api", "chains", "etl", "llm", "scripts", "utils")


def _python_files() -> list[Path]:
    files: list[Path] = []
    for dirname in SCANNED_DIRS:
        files.extend(sorted((REPO_ROOT / dirname).glob("**/*.py")))
    return files


def test_consolidated_helpers_are_not_redefined_locally() -> None:
    duplicates: list[str] = []
    home_definitions = dict.fromkeys(CONSOLIDATED_HELPERS, 0)
    for path in _python_files():
        relative = path.relative_to(REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                expected_home = CONSOLIDATED_HELPERS.get(node.name)
                if expected_home is None:
                    continue
                if relative == expected_home:
                    home_definitions[node.name] += 1
                else:
                    duplicates.append(f"{relative}:{node.lineno}:{node.name}")

    assert duplicates == []
    missing = [
        f"{name} -> {home}"
        for name, home in CONSOLIDATED_HELPERS.items()
        if not name.startswith("_") and home_definitions[name] == 0
    ]
    assert missing == []
