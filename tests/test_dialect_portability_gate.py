from __future__ import annotations

from pathlib import Path

from scripts.check_dialect_portability import AUDITED_SQLITE_ONLY_ALLOWLIST, scan


def test_dialect_gate_accepts_current_audited_repo_state() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    findings = scan([repo_root], repo_root=repo_root, allowlist=AUDITED_SQLITE_ONLY_ALLOWLIST)

    assert findings == []


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
