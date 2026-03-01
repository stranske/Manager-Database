import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "seed_universe.py"


def _run_seed(
    tmp_path: Path, input_path: Path, *extra_args: str
) -> subprocess.CompletedProcess[str]:
    db_path = tmp_path / "seed.db"
    env = {**os.environ, "DB_PATH": str(db_path)}
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--file", str(input_path), *extra_args],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=env,
    )


def _fetch_rows(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT name, cik, jurisdiction FROM managers ORDER BY cik").fetchall()
    finally:
        conn.close()


def test_seed_universe_json_upsert_is_idempotent(tmp_path):
    payload = [
        {"name": "Berkshire Hathaway", "cik": "0001067983", "jurisdiction": "us"},
        {"name": "Bridgewater Associates", "cik": "0001350694", "jurisdiction": "us"},
    ]
    input_path = tmp_path / "universe.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    first = _run_seed(tmp_path, input_path)
    assert "Created: 2" in first.stdout
    assert "Updated: 0" in first.stdout

    second = _run_seed(tmp_path, input_path)
    assert "Created: 0" in second.stdout
    assert "Updated: 2" in second.stdout

    rows = _fetch_rows(tmp_path / "seed.db")
    assert rows == [
        ("Berkshire Hathaway", "0001067983", "us"),
        ("Bridgewater Associates", "0001350694", "us"),
    ]


def test_seed_universe_csv_and_dry_run(tmp_path):
    input_path = tmp_path / "universe.csv"
    input_path.write_text(
        "name,cik,jurisdiction\n"
        "Citadel Advisors,0001423053,us\n"
        "TCI Fund Management,0001372663,uk\n",
        encoding="utf-8",
    )

    dry_run = _run_seed(tmp_path, input_path, "--dry-run")
    assert "Dry run complete. No rows written." in dry_run.stdout
    assert "Created: 2" in dry_run.stdout

    db_path = tmp_path / "seed.db"
    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM managers").fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_sample_universe_json_contains_confirmed_managers_and_seeds(tmp_path):
    sample_path = REPO_ROOT / "scripts" / "sample_universe.json"
    payload = json.loads(sample_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert len(payload) >= 10

    by_cik = {item["cik"]: item for item in payload}
    assert "0001791786" in by_cik
    assert "0001434997" in by_cik
    assert by_cik["0001791786"]["name"] == "Elliott Investment Management L.P."
    assert by_cik["0001434997"]["name"] == "SIR Capital Management L.P."

    result = _run_seed(tmp_path, sample_path)
    assert "Created: 10" in result.stdout
    assert "Updated: 0" in result.stdout
