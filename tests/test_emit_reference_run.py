"""The shell emitter wired into backplane-conformance.yml produces a conformant run."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EMITTER = REPO_ROOT / "scripts" / "emit_reference_run.sh"
VALIDATOR = REPO_ROOT / "scripts" / "validate_run_contract.py"
REPO = "stranske/Manager-Database"


def _emit(out_dir: Path) -> None:
    bin_dir = Path(sys.executable).parent
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin"}
    subprocess.run([str(EMITTER), str(out_dir)], check=True, cwd=REPO_ROOT, env=env)


def _emitting_registry(path: Path) -> Path:
    # The Workflows registry lists Manager-Database as ``planned``, which the
    # validator skips. Mark it ``emitting`` so the envelope is actually checked.
    registry = {
        "participants": [
            {
                "repo": REPO,
                "contract_version": "run-contract/v1",
                "role": "producer",
                "artifact_name": "run.json",
                "manifest_name": "manifest.json",
                "required_sections": ["identity_refs"],
                "status": "emitting",
            }
        ]
    }
    path.write_text(json.dumps(registry), encoding="utf-8")
    return path


def test_emit_reference_run_produces_valid_run_contract(tmp_path: Path) -> None:
    out_dir = tmp_path / "reference"
    _emit(out_dir)

    assert EMITTER.stat().st_mode & 0o111, "emit_reference_run.sh must be executable"
    run_json = out_dir / "run.json"
    manifest = out_dir / "manifest.json"
    report_json = tmp_path / "report.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            str(run_json),
            "--manifest",
            str(manifest),
            "--registry",
            str(_emitting_registry(tmp_path / "registry.json")),
            "--repo",
            REPO,
            "--report-json",
            str(report_json),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert completed.returncode == 0, completed.stderr
    assert report["skipped"] is False
    assert report["conformant"] is True
    envelope = json.loads(run_json.read_text(encoding="utf-8"))
    assert envelope["schema_version"] == "run-contract/v1"
    assert envelope["outputs"]["summary"] == {
        "delta_count": 4,
        "by_type": {"ADD": 1, "DECREASE": 1, "EXIT": 1, "INCREASE": 1},
    }


def test_emit_reference_run_is_deterministic(tmp_path: Path) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    _emit(first)
    _emit(second)

    for name in ("run.json", "manifest.json", "deltas.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
