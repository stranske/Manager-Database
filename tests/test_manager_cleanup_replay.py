"""Run the cleanup mutation proof in both interpreter modes as a regression check."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from defusedxml import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / "docs/evidence/issue-1750-cleanup-propagation/replay_cleanup.py"
TEST = "tests/test_manager_bulk_cleanup_failure.py"
CASE = "test_bulk_import_reports_autocommit_restore_failure_after_commit"


def file_hashes(paths):
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


@pytest.mark.parametrize("optimized", [False, True], ids=["standard", "optimized"])
def test_cleanup_replay_detects_mutation_restores_source_and_refuses_reuse(tmp_path, optimized):
    pytest.importorskip("psycopg")
    inputs = [ROOT / "api/managers.py", ROOT / TEST]
    before = file_hashes(inputs)
    output = tmp_path / "proof"
    argv = [sys.executable, *(["-O"] if optimized else []), str(DRIVER), "--output", str(output)]

    result = subprocess.run(
        argv, cwd=ROOT, capture_output=True, text=True, timeout=300, check=False
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS: one named RED1/restored GREEN0 pair" in result.stdout
    assert file_hashes(inputs) == before
    controls = json.loads((output / "controls.json").read_text())
    assert controls["caller_identity"] == {
        str(path.relative_to(ROOT)): before[str(path)] for path in inputs
    }
    assert controls["replay_sha256"] == hashlib.sha256(DRIVER.read_bytes()).hexdigest()
    assert len(controls["controls"]) == 1
    control = controls["controls"][0]
    assert control["node"] == f"{TEST}::{CASE}"
    assert control["original_sha256"] == before[str(inputs[0])]
    assert control["mutated_sha256"] != control["original_sha256"]
    assert control["restored_sha256"] == control["original_sha256"]
    for phase, expected_exit in [("red", 1), ("green", 0)]:
        receipt = json.loads((output / f"{CASE}-{phase}.json").read_text())
        assert receipt == control[phase]
        assert receipt["exit"] == expected_exit
        assert ("-O" in receipt["argv"]) is optimized
        assert receipt["cwd"] != str(ROOT)
        cases = list(ET.parse(output / f"{CASE}-{phase}.xml").getroot().iter("testcase"))
        assert len(cases) == 1
        assert cases[0].get("name") == CASE
        assert not list(cases[0].iter("error"))
        assert not list(cases[0].iter("skipped"))
        failures = list(cases[0].iter("failure"))
        assert len(failures) == (1 if phase == "red" else 0)
        if phase == "red":
            assert "assert 200 == 503" in failures[0].text

    proof_before = file_hashes(output.iterdir())
    assert len(proof_before) == 7
    refused = subprocess.run(
        argv, cwd=ROOT, capture_output=True, text=True, timeout=30, check=False
    )
    assert refused.returncode == 1
    assert "FileExistsError" in refused.stderr
    assert "PASS:" not in refused.stdout
    assert file_hashes(output.iterdir()) == proof_before
    assert file_hashes(inputs) == before
