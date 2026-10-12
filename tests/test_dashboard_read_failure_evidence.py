"""Check dashboard acceptance claims against immutable raw experiment receipts."""

import ast
import hashlib
import json
import tarfile
from collections import Counter
from pathlib import Path

import pytest
from defusedxml import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/issue-1750-dashboard-read-failures"
SOURCE = "ui/dashboard.py"
TEST = "tests/test_dashboard_loader_failures.py"
DRIVER = "docs/evidence/issue-1750-dashboard-read-failures/dashboard_read_failure_replay.py"
SQL = ("load_filing_timeline", "load_latest_holdings_snapshot", "load_top_deltas")
DOMAIN = (
    "load_manager_activism_filings",
    "load_manager_activism_events",
    "load_manager_activism_timeline",
    "load_active_campaigns_summary",
    "load_manager_conviction_scores",
    "load_manager_crowded_trades",
    "load_manager_contrarian_signals",
)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def case_name(loader):
    test = (
        "test_sql_loader_returns_typed_empty_frame_and_closes_missing_schema"
        if loader in SQL
        else "test_domain_loader_returns_empty_and_closes_after_query_error"
    )
    return f"{test}[{loader}]"


def outcomes(raw):
    result = {}
    for case in ET.fromstring(raw).iter("testcase"):
        node = f"{case.get('classname')}::{case.get('name')}"
        assert node not in result, f"Duplicate test node: {node}"
        assert case.find("error") is None, f"Test error: {node}"
        failure, skip = case.find("failure"), case.find("skipped")
        assert failure is None or skip is None, f"Ambiguous outcome: {node}"
        result[node] = (
            "failure" if failure is not None else "skipped" if skip is not None else "passed"
        )
    return result


@pytest.fixture(scope="module")
def archive():
    with tarfile.open(EVIDENCE / "validation.tar.gz") as bundle:
        members = bundle.getmembers()
        assert len(members) == 96
        assert all(member.isfile() for member in members)
        assert len({member.name for member in members}) == len(members)
        return {member.name: bundle.extractfile(member).read() for member in members}


def test_dashboard_archive_and_current_source_bindings(archive):
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    assert digest((EVIDENCE / "validation.tar.gz").read_bytes()) == manifest["archive_sha256"]
    assert set(archive) == set(manifest["members"])
    for name, raw in archive.items():
        assert digest(raw) == manifest["members"][name], name
    assert set(manifest["current_bindings"]) == {SOURCE, TEST, DRIVER}
    for name, expected in manifest["current_bindings"].items():
        assert digest((ROOT / name).read_bytes()) == expected, name
    for name, expected in manifest["historical_bindings"].items():
        assert digest(archive[f"source/{name}"]) == expected, name
        if name in (SOURCE, TEST):
            assert archive[f"source/{name}"] == (ROOT / name).read_bytes(), name
    proof = json.loads(archive["mutations/controls.json"])
    assert proof["caller_identity"] == {
        name: manifest["current_bindings"][name] for name in (SOURCE, TEST)
    }
    assert (
        proof["replay_sha256"]
        == manifest["historical_bindings"][
            "docs/evidence/issue-1750-dashboard-read-failures/replay.py"
        ]
    )


@pytest.mark.parametrize("loader", SQL + DOMAIN)
def test_dashboard_ten_intended_red_and_restored_green_pairs(archive, loader):
    controls = json.loads(archive["mutations/controls.json"])["controls"]
    expected_nodes = {f"{TEST}::{case_name(name)}" for name in SQL + DOMAIN}
    assert len(controls) == 10
    assert {control["node"] for control in controls} == expected_nodes
    control = next(
        control for control in controls if control["node"] == f"{TEST}::{case_name(loader)}"
    )
    original = archive[f"mutations/{loader}-original.py"]
    mutant = archive[f"mutations/{loader}-mutant.py"]
    assert original == archive[f"source/{SOURCE}"]
    assert digest(original) == control["original_sha256"] == control["restored_sha256"]
    assert digest(mutant) == control["mutant_sha256"] != digest(original)

    # Independently limit each mutation to the selected production error fallback.
    function = next(
        node
        for node in ast.parse(original).body
        if isinstance(node, ast.FunctionDef) and node.name == loader
    )
    handlers = [node for node in ast.walk(function) if isinstance(node, ast.ExceptHandler)]
    assert len(handlers) == 1
    fallback = handlers[0].body[0]
    assert isinstance(fallback, ast.Assign)
    replacement = (
        'df = pd.DataFrame({"unexpected": [1]})'
        if loader in SQL
        else 'return pd.DataFrame({"unexpected": [1]})'
    )
    lines = original.decode().splitlines(keepends=True)
    expected_mutant = (
        "".join(lines[: fallback.lineno - 1])
        + " " * fallback.col_offset
        + replacement
        + "\n"
        + "".join(lines[fallback.end_lineno :])
    ).encode()
    assert mutant == expected_mutant
    assert control["red"]["cwd"] == control["green"]["cwd"]
    for phase, expected_exit, status in (("red", 1, "failure"), ("green", 0, "passed")):
        prefix = f"mutations/{loader}-{phase}"
        receipt = json.loads(archive[f"{prefix}.json"])
        assert receipt == control[phase]
        assert receipt["exit"] == expected_exit
        assert receipt["argv"][1:4] == ["-m", "pytest", control["node"]]
        xml_arg = next(arg for arg in receipt["argv"] if arg.startswith("--junitxml="))
        assert Path(xml_arg.split("=", 1)[1]).name == f"{loader}-{phase}.xml"
        xml = archive[f"{prefix}.xml"]
        assert outcomes(xml) == {
            f"tests.test_dashboard_loader_failures::{case_name(loader)}": status
        }
        suite = ET.fromstring(xml).find("testsuite")
        assert {key: suite.get(key) for key in ("tests", "errors", "failures", "skipped")} == {
            "tests": "1",
            "errors": "0",
            "failures": str(expected_exit),
            "skipped": "0",
        }
        if phase == "red":
            failure = suite.find("testcase/failure")
            assert failure.get("message", "").startswith("assert False")
            assert ">       assert result.empty" in failure.text
            assert "AssertionError" in failure.text
        assert archive[f"{prefix}.log"], "Missing raw pytest output"


def test_dashboard_archived_full_suites_preserve_all_old_outcomes(archive):
    comparison = json.loads((EVIDENCE / "comparison.json").read_text())
    baseline = outcomes(archive["md-baseline-junit.xml"])
    candidate = outcomes(archive["md-candidate-junit.xml"])
    assert set(baseline) <= set(candidate)
    assert {node: candidate[node] for node in baseline} == baseline
    added = {node: status for node, status in candidate.items() if node not in baseline}
    assert (
        added
        == comparison["new_nodes"]
        == {
            f"tests.test_dashboard_loader_failures::{case_name(loader)}": "passed"
            for loader in SQL + DOMAIN
        }
    )
    assert comparison["old_outcome_changes"] == {}
    for phase, results, passed in (("baseline", baseline, 1959), ("candidate", candidate, 1969)):
        assert (
            Counter(results.values())
            == comparison[f"{phase}_outcomes"]
            == {
                "passed": passed,
                "failure": 24,
                "skipped": 21,
            }
        )
        command = json.loads(archive[f"md-{phase}-command.json"])
        assert command["exit"] == 1
        assert command["argv"][1:5] == ["-m", "pytest", "tests", "-q"]
        assert "--cov" in command["argv"]
        assert command["checkout_base"] == comparison["base_head"]
    assert all(
        node.startswith("tests.test_ui_auth::")
        for node, status in baseline.items()
        if status == "failure"
    )


def test_dashboard_archived_statement_coverage_preserves_every_old_line(archive):
    comparison = json.loads((EVIDENCE / "comparison.json").read_text())
    baseline = json.loads(archive["md-baseline-coverage.json"])
    candidate = json.loads(archive["md-candidate-coverage.json"])
    assert set(baseline["files"]) == set(candidate["files"])
    assert len(candidate["files"]) == comparison["source_file_count"] == 117
    assert comparison["identical_source_universe"] is True
    assert comparison["same_exclusions"] is True
    assert comparison["covered_line_regressions"] == {}
    assert comparison["coverage_branch_measured"] is False
    gained = {}
    for name, old in baseline["files"].items():
        new = candidate["files"][name]
        assert set(old["executed_lines"]) <= set(new["executed_lines"]), name
        assert old["excluded_lines"] == new["excluded_lines"], name
        assert set(old["executed_lines"] + old["missing_lines"]) == set(
            new["executed_lines"] + new["missing_lines"]
        ), name
        added = set(new["executed_lines"]) - set(old["executed_lines"])
        if added:
            gained[name] = added
    assert set(gained) == {SOURCE}
    assert len(gained[SOURCE]) == 20
    for phase, data, covered, percent in (
        ("baseline", baseline, 12447, 86.98721085),
        ("candidate", candidate, 12467, 87.12698302),
    ):
        assert data["meta"]["branch_coverage"] is False
        totals = data["totals"]
        assert totals == comparison[phase]
        assert totals["covered_lines"] == covered
        assert totals["num_statements"] == 14309
        assert totals["excluded_lines"] == 82
        assert round(totals["percent_covered"], 8) == percent
        assert totals["percent_covered"] == pytest.approx(100 * covered / 14309)
        assert sum(len(file["executed_lines"]) for file in data["files"].values()) == covered
        assert sum(len(file["missing_lines"]) for file in data["files"].values()) == 14309 - covered
        assert sum(len(file["excluded_lines"]) for file in data["files"].values()) == 82


def test_dashboard_archived_reuse_refusal_and_black_receipt(archive):
    refusal = json.loads(archive["md-reuse.json"])
    assert refusal["exit"] == 1
    assert "FileExistsError" in refusal["stderr"]
    assert "output.mkdir(parents=True, exist_ok=False)" in refusal["stderr"]
    assert refusal["original_evidence_hashes_unchanged"] is True
    assert refusal["production_bytes_unchanged"] is True
    assert "409 files would be left unchanged." in archive["md-black.log"].decode()
