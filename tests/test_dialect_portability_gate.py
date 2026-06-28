from __future__ import annotations

from pathlib import Path

from scripts.check_dialect_portability import (
    AUDITED_SQLITE_ONLY_ALLOWLIST,
    _imports_connect_db,
    _python_files,
    _relative_path,
    allowlist_paths_missing_from_audit,
    documented_audit_paths,
    scan,
)


def test_dialect_gate_accepts_current_audited_repo_state() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    findings = scan([repo_root], repo_root=repo_root, allowlist=AUDITED_SQLITE_ONLY_ALLOWLIST)

    assert findings == []


def test_etl_1008_surfaces_pass_without_allowlist_entries() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    etl_surfaces = [
        repo_root / "etl" / "ingest_flow.py",
        repo_root / "etl" / "conviction_flow.py",
        repo_root / "etl" / "evaluation_flow.py",
    ]

    findings = scan(etl_surfaces, repo_root=repo_root, allowlist={})

    assert findings == []


def test_api_alert_1009_surfaces_have_no_allowlist_entries() -> None:
    assert "alerts/db.py" not in AUDITED_SQLITE_ONLY_ALLOWLIST
    assert "api/chat.py" not in AUDITED_SQLITE_ONLY_ALLOWLIST
    assert "api/managers.py" not in AUDITED_SQLITE_ONLY_ALLOWLIST
    assert "api/search.py" not in AUDITED_SQLITE_ONLY_ALLOWLIST
    assert "api/signals.py" not in AUDITED_SQLITE_ONLY_ALLOWLIST


def test_dialect_gate_rejects_unaudited_sqlite_only_token(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def migrate() -> None:",
                "    conn = connect_db()",
                "    conn.execute('CREATE TABLE x (id INTEGER PRIMARY KEY AUTOINCREMENT)')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert len(findings) == 1
    assert findings[0].path == "feature.py"
    assert findings[0].token == "AUTOINCREMENT"


def test_dialect_gate_honors_documented_allowlist(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def migrate() -> None:",
                "    conn = connect_db()",
                "    conn.execute('PRAGMA table_info(managers)')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={"feature.py": {"PRAGMA table_info"}})

    assert findings == []


def test_dialect_gate_reports_table_xinfo_as_distinct_token(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def inspect() -> None:",
                "    conn = connect_db()",
                "    conn.execute('PRAGMA table_xinfo(managers)')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert len(findings) == 1
    assert findings[0].token == "PRAGMA table_xinfo"


def test_dialect_gate_rejects_unconditional_sqlite_placeholder(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def query() -> None:",
                "    conn = connect_db()",
                "    conn.execute('SELECT * FROM managers WHERE cik = ?', ('1',))",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert len(findings) == 1
    assert findings[0].token == "SQLITE ? placeholder"


def test_dialect_gate_rejects_legacy_daily_diff_schema_names(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def query() -> None:",
                "    conn = connect_db()",
                "    conn.execute('SELECT d.change FROM daily_diff d')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert [finding.token for finding in findings] == [
        "legacy daily_diff table",
        "legacy d.change column",
    ]


def test_allowlist_paths_missing_from_audit_reports_missing_entry(tmp_path: Path) -> None:
    report = tmp_path / "docs" / "reports" / "dialect_portability_audit.md"
    report.parent.mkdir(parents=True)
    report.write_text(
        "\n".join(
            [
                "# Dialect Portability Audit",
                "",
                "| Surface | Classification | Evidence | Failure Mode Or Justification | Disposition |",
                "| --- | --- | --- | --- | --- |",
                "| `api/chat.py` | postgres-incompatible | `PRAGMA table_info` | sample | Follow-up #1009 |",
            ]
        ),
        encoding="utf-8",
    )

    missing = allowlist_paths_missing_from_audit(
        repo_root=tmp_path,
        allowlist={"api/chat.py": {"PRAGMA table_info"}, "api/search.py": {"PRAGMA table_info"}},
    )

    assert missing == ["api/search.py"]


def test_allowlist_paths_missing_from_audit_accepts_documented_entries(tmp_path: Path) -> None:
    report = tmp_path / "docs" / "reports" / "dialect_portability_audit.md"
    report.parent.mkdir(parents=True)
    report.write_text(
        "\n".join(
            [
                "# Dialect Portability Audit",
                "",
                "| Surface | Classification | Evidence | Failure Mode Or Justification | Disposition |",
                "| --- | --- | --- | --- | --- |",
                "| `api/chat.py` | postgres-incompatible | `PRAGMA table_info` | sample | Follow-up #1009 |",
                "| `api/search.py` | postgres-incompatible | `PRAGMA table_info` | sample | Follow-up #1009 |",
            ]
        ),
        encoding="utf-8",
    )

    missing = allowlist_paths_missing_from_audit(
        repo_root=tmp_path,
        allowlist={"api/chat.py": {"PRAGMA table_info"}, "api/search.py": {"PRAGMA table_info"}},
    )

    assert missing == []


def test_relative_path_returns_relative_to_repo_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    file_path = repo_root / "src" / "module.py"
    file_path.parent.mkdir()
    file_path.write_text("")

    result = _relative_path(file_path, repo_root)
    assert result == "src/module.py"


def test_relative_path_returns_absolute_when_outside_repo(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    outside_path = tmp_path / "outside" / "module.py"
    outside_path.parent.mkdir()
    outside_path.write_text("")

    result = _relative_path(outside_path, repo_root)
    assert result == outside_path.as_posix()


def test_python_files_returns_sorted_unique_python_files(tmp_path: Path) -> None:
    # Create some Python files
    (tmp_path / "module1.py").write_text("")
    (tmp_path / "module2.py").write_text("")
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "module3.py").write_text("")
    # Create a directory to scan
    scan_dir = tmp_path / "scan_dir"
    scan_dir.mkdir()
    (scan_dir / "module4.py").write_text("")
    # Create non-Python files that should be ignored
    (scan_dir / "readme.txt").write_text("")
    (scan_dir / "__pycache__").mkdir()
    (scan_dir / "__pycache__" / "cached.py").write_text("")
    (scan_dir / "tests").mkdir()
    (scan_dir / "tests" / "test_module.py").write_text("")
    (scan_dir / ".venv").mkdir()
    (scan_dir / ".venv" / "vendored.py").write_text("")
    (scan_dir / "check_dialect_portability.py").write_text("")

    files = _python_files([tmp_path, scan_dir])
    paths = [f.relative_to(tmp_path).as_posix() for f in files]

    # Should include all .py files except those in tests/ or __pycache__
    assert "module1.py" in paths
    assert "module2.py" in paths
    assert "subdir/module3.py" in paths
    assert "scan_dir/module4.py" in paths
    assert "__pycache__/cached.py" not in paths
    assert "tests/test_module.py" not in paths
    assert "scan_dir/.venv/vendored.py" not in paths
    assert "scan_dir/check_dialect_portability.py" not in paths
    # Should be sorted and unique
    assert paths == sorted(paths)
    assert len(paths) == len(set(paths))


def test_imports_connect_db_detects_import_from_adapters_base() -> None:
    source = """from adapters.base import connect_db

def func():
    pass
"""
    assert _imports_connect_db(source) is True


def test_imports_connect_db_detects_function_def() -> None:
    source = """def connect_db():
    pass
"""
    assert _imports_connect_db(source) is True


def test_imports_connect_db_detects_direct_call() -> None:
    source = """conn = connect_db()
"""
    assert _imports_connect_db(source) is True


def test_imports_connect_db_detects_direct_call_when_source_is_malformed() -> None:
    source = """def broken(
    conn = connect_db()
"""
    assert _imports_connect_db(source) is True


def test_imports_connect_db_ignores_non_connect_db() -> None:
    source = """def some_other_function():
    pass
"""
    assert _imports_connect_db(source) is False


def test_documented_audit_paths_returns_module_paths_from_table(tmp_path: Path) -> None:
    report = tmp_path / "docs" / "reports" / "dialect_portability_audit.md"
    report.parent.mkdir(parents=True)
    report.write_text(
        "\n".join(
            [
                "# Dialect Portability Audit",
                "",
                "| Surface | Classification | Evidence | Failure Mode Or Justification | Disposition |",
                "| --- | --- | --- | --- | --- |",
                "| `api/chat.py` | dialect-aware | evidence | justification | disposition |",
                "| `etl/activism_flow.py` | dialect-aware | evidence | justification | disposition |",
                "| Some other text with `not_a_module.py` in backticks | more | stuff |",
            ]
        ),
        encoding="utf-8",
    )

    paths = documented_audit_paths(report)
    assert paths == {"api/chat.py", "etl/activism_flow.py"}


def test_documented_audit_paths_returns_empty_set_for_nonexistent_report(tmp_path: Path) -> None:
    report = tmp_path / "nonexistent" / "report.md"
    paths = documented_audit_paths(report)
    assert paths == set()


def test_dialect_gate_rejects_insert_or_ignore(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def migrate() -> None:",
                "    conn = connect_db()",
                "    conn.execute('INSERT OR IGNORE INTO managers (id, name) VALUES (1, \"test\")')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert len(findings) == 1
    assert findings[0].path == "feature.py"
    assert findings[0].token == "INSERT OR IGNORE"


def test_dialect_gate_rejects_pragma_table_info(tmp_path: Path) -> None:
    module = tmp_path / "feature.py"
    module.write_text(
        "\n".join(
            [
                "from adapters.base import connect_db",
                "",
                "def inspect() -> None:",
                "    conn = connect_db()",
                "    conn.execute('PRAGMA table_info(managers)')",
            ]
        ),
        encoding="utf-8",
    )

    findings = scan([module], repo_root=tmp_path, allowlist={})

    assert len(findings) == 1
    assert findings[0].path == "feature.py"
    assert findings[0].token == "PRAGMA table_info"
