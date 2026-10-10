"""Reject unrelated RED failures and restore source on interrupted proof runs."""

import importlib.util
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

DRIVER = (
    Path(__file__).resolve().parents[1]
    / "docs/evidence/issue-1750-legacy-schema/replay_schema_boundaries.py"
)
MARKERS = {
    "json-default": "legacy JSON defaults changed",
    "created-backfill": "legacy created_at backfill missing",
    "updated-backfill": "legacy updated_at backfill missing",
    "unique-index": "legacy CIK uniqueness index missing",
}


@pytest.fixture
def replay():
    spec = importlib.util.spec_from_file_location("schema_replay_control", DRIVER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", MARKERS)
@pytest.mark.parametrize("intended", [False, True], ids=["unrelated-exception", "assertion"])
def test_red_requires_the_selected_regression_assertion(
    replay, monkeypatch, tmp_path, name, intended
):
    node = next(case[1] for case in replay.CASES if case[0] == name)

    def run(argv, **kwargs):
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        root = ET.Element("testsuite")
        case = ET.SubElement(root, "testcase", name=node)
        message = (
            f"AssertionError: {MARKERS[name]}" if intended else "RuntimeError: unrelated crash"
        )
        ET.SubElement(case, "failure", message=message).text = message
        ET.ElementTree(root).write(junit)
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")

    monkeypatch.setattr(replay.subprocess, "run", run)
    assert replay.run_case(tmp_path, name, node, "red")["valid"] is intended


@pytest.mark.parametrize("failure", ["timeout", "malformed-junit"])
def test_main_restores_source_and_never_claims_success_on_red_failure(
    replay, monkeypatch, tmp_path, capsys, failure
):
    original = replay.SOURCE.read_bytes()
    source = tmp_path / "managers.py"
    source.write_bytes(original)
    monkeypatch.setattr(replay, "SOURCE", source)
    output = tmp_path / "proof"
    monkeypatch.setattr(sys, "argv", [str(DRIVER), "--output", str(output)])

    def run(argv, **kwargs):
        assert source.read_bytes() != original
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 90)
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        junit.write_text("<broken")
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")

    monkeypatch.setattr(replay.subprocess, "run", run)
    expected = subprocess.TimeoutExpired if failure == "timeout" else ET.ParseError
    with pytest.raises(expected):
        replay.main()
    assert source.read_bytes() == original
    receipt = json.loads((output / "receipts.json").read_text())
    assert receipt["restored"] is True
    assert receipt["callers_unchanged"] is True
    assert receipt["receipts"] == []
    assert "Four production mutations" not in capsys.readouterr().out
