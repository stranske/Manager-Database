"""Emit the Manager-Database ``run-contract/v1`` reference run.

Runs ``diff_holdings`` against the checked-in fixture in an in-memory SQLite
database and writes ``run.json`` plus an ``artifact-manifest/v1``
``manifest.json`` (and the ``deltas.json`` artifact it lists) into the output
directory. The run is deterministic: the run id is content-addressed from the
fixture bytes, and no wall-clock timestamps are written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from diff_holdings import diff_holdings  # noqa: E402
from tools.run_contract import RunResult  # noqa: E402

REPO = "stranske/Manager-Database"
GITHUB_ISSUE = "stranske/Manager-Database#1849"
TOOL = "diff-holdings"
TOOL_VERSION = "0.1.0"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "reference_run" / "diff_holdings.json"
DELTAS_ARTIFACT_ID = "diff-holdings-deltas"


def _seed_connection(fixture: dict[str, Any]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    manager = fixture["manager"]
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?,?,?)",
        (manager["manager_id"], manager["name"], manager["cik"]),
    )
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?,?,?,?,?)",
        [
            (f["filing_id"], manager["manager_id"], f["type"], f["filed_date"], f["source"])
            for f in fixture["filings"]
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?,?,?,?,?)",
        [
            (h["filing_id"], h["cusip"], h["name_of_issuer"], h["shares"], h["value_usd"])
            for h in fixture["holdings"]
        ],
    )
    return conn


def _dump(payload: Any) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def emit_reference_run(out_dir: Path, fixture_path: Path = FIXTURE_PATH) -> dict[str, Any]:
    """Write ``run.json``, ``manifest.json`` and ``deltas.json`` into ``out_dir``."""
    fixture_bytes = fixture_path.read_bytes()
    fixture = json.loads(fixture_bytes)
    manager = fixture["manager"]

    conn = _seed_connection(fixture)
    try:
        result = diff_holdings(int(manager["manager_id"]), conn)
    finally:
        conn.close()

    run_id = hashlib.sha256(TOOL.encode("utf-8") + b"|" + fixture_bytes).hexdigest()
    result = RunResult(
        run_id=run_id,
        tool=TOOL,
        requested_by="ci",
        reason="backplane reference run",
        inputs={
            "manager_id": result.inputs.get("manager_id", manager["manager_id"]),
            "fixture_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
        },
        outputs=result.outputs,
        warnings=result.warnings,
        status=result.status,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    deltas_bytes = _dump(result.outputs)
    (out_dir / "deltas.json").write_bytes(deltas_bytes)
    manifest = {
        "schema_version": "artifact-manifest/v1",
        "run_id": run_id,
        "tool": TOOL,
        "artifacts": [
            {
                "artifact_id": DELTAS_ARTIFACT_ID,
                "name": "deltas.json",
                "kind": "data",
                "path": "deltas.json",
                "sha256": hashlib.sha256(deltas_bytes).hexdigest(),
            }
        ],
    }
    (out_dir / "manifest.json").write_bytes(_dump(manifest))

    delta_counts = Counter(delta["delta_type"] for delta in result.outputs)
    envelope = result.to_run_contract(
        repo=REPO,
        github_issue=GITHUB_ISSUE,
        actor={"kind": "ci", "id": "backplane-conformance", "intent": "reference run"},
        tool_version=TOOL_VERSION,
        artifact_ids=[DELTAS_ARTIFACT_ID],
        identity_refs=[f"manager:cik_{manager['cik']}"],
        outputs_summary={"delta_count": len(result.outputs), "by_type": dict(delta_counts)},
    )
    (out_dir / "run.json").write_bytes(_dump(envelope))
    return envelope


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_dir", type=Path)
    args = parser.parse_args(argv)
    envelope = emit_reference_run(args.out_dir)
    print(f"wrote {args.out_dir / 'run.json'} run_id={envelope['run_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
