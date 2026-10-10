"""Verify ingress acceptance claims against the lossless archived raw evidence."""

import hashlib
import json
import tarfile
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/issue-1750-ingress"
NODE_FAILURES = {
    "test_invalid_bulk_limit_uses_safe_default_and_warns": "AssertionError: invalid limit lost safe default",
    "test_empty_csv_reports_missing_name_header_before_storage": "AssertionError: empty CSV lost missing-header diagnosis",
    "test_invalid_patch_cik_is_rejected_before_storage": "AssertionError: invalid request reached database",
    "test_invalid_content_length_still_bounds_actual_bytes_before_storage": "AssertionError: actual bytes were not bounded",
}


@pytest.fixture(scope="module")
def archive():
    with tarfile.open(EVIDENCE / "validation.tar.gz") as bundle:
        members = bundle.getmembers()
        assert all(member.isfile() for member in members)
        assert len({member.name for member in members}) == len(members)
        return {member.name: bundle.extractfile(member).read() for member in members}


def outcomes(raw):
    result = {}
    for case in ET.fromstring(raw).findall(".//testcase"):
        node = f"{case.get('classname')}::{case.get('name')}"
        assert node not in result, f"Duplicate archived test node: {node}"
        assert case.find("error") is None, f"Collection/setup error: {node}"
        result[node] = (
            "failed"
            if case.find("failure") is not None
            else "skipped" if case.find("skipped") is not None else "passed"
        )
    return result


def test_ingress_archive_and_reviewed_snapshots_match_sha256_bindings(archive):
    index = json.loads((EVIDENCE / "archive-index.json").read_text())
    assert (
        hashlib.sha256((EVIDENCE / "validation.tar.gz").read_bytes()).hexdigest()
        == index["archive_sha256"]
    )
    assert set(archive) == set(index["members"])
    for name, raw in archive.items():
        assert hashlib.sha256(raw).hexdigest() == index["members"][name], name
    assert index["current_bindings"] == {
        name: hashlib.sha256(archive[member]).hexdigest()
        for name, member in [
            ("api/managers.py", "original-source.py"),
            ("tests/test_manager_ingress_boundaries.py", "candidate-tests.py"),
            ("docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py", "replay-source.py"),
        ]
    }
    assert archive["comparison.json"] == (EVIDENCE / "comparison.json").read_bytes()


def test_ingress_archived_suite_adds_only_four_passing_cases(archive):
    comparison = json.loads(archive["comparison.json"])
    baseline = outcomes(archive["baseline/results.xml"])
    candidate = outcomes(archive["candidate/results.xml"])
    assert set(baseline) <= set(candidate)
    assert {node: candidate[node] for node in baseline} == baseline
    expected_new = {
        f"tests.test_manager_ingress_boundaries::{node}": "passed" for node in NODE_FAILURES
    }
    assert {
        node: status for node, status in candidate.items() if node not in baseline
    } == expected_new
    assert comparison["new_cases"] == expected_new
    assert comparison["old_outcome_changes"] == {}
    assert outcomes(archive["focused.xml"]) == expected_new
    for phase, results, expected in [
        ("baseline", baseline, {"passed": 1827, "failed": 24, "skipped": 21}),
        ("candidate", candidate, {"passed": 1831, "failed": 24, "skipped": 21}),
    ]:
        assert Counter(results.values()) == expected == comparison["outcome_counts"][phase]
        command = json.loads(archive[f"{phase}/command.json"])
        assert command == comparison[f"{phase}_command"]
        assert command["exit"] == 1
    failures = {node for node, status in candidate.items() if status == "failed"}
    assert failures == set(comparison["existing_failure_nodes"])
    assert all(node.startswith("tests.test_ui_auth::") for node in failures)


def test_ingress_archived_coverage_gains_six_manager_lines_without_losses(archive):
    baseline = json.loads(archive["baseline/coverage.json"])
    candidate = json.loads(archive["candidate/coverage.json"])
    comparison = json.loads(archive["comparison.json"])
    assert set(baseline["files"]) == set(candidate["files"])
    assert len(candidate["files"]) == comparison["file_count"] == 116
    gained = {}
    for name, old in baseline["files"].items():
        new = candidate["files"][name]
        assert set(old["executed_lines"]) <= set(new["executed_lines"]), name
        assert old["excluded_lines"] == new["excluded_lines"], name
        assert set(old["executed_lines"] + old["missing_lines"]) == set(
            new["executed_lines"] + new["missing_lines"]
        ), name
        added = sorted(set(new["executed_lines"]) - set(old["executed_lines"]))
        if added:
            gained[name] = added
    assert (
        gained
        == comparison["new_covered_lines"]
        == {"api/managers.py": [837, 838, 839, 847, 1310, 1311]}
    )
    assert comparison["lost_covered_lines"] == {}
    for phase, data, covered in [("baseline", baseline, 12356), ("candidate", candidate, 12362)]:
        totals = data["totals"]
        assert totals == comparison[f"{phase}_totals"]
        assert totals["num_statements"] == 14238
        assert totals["excluded_lines"] == 82
        assert totals["covered_lines"] == covered
        assert sum(len(file["executed_lines"]) for file in data["files"].values()) == covered
        assert totals["percent_covered"] == pytest.approx(100 * covered / 14238)


def test_ingress_archived_mutations_have_named_red_and_restored_green_results(archive):
    proof = json.loads(archive["mutations/receipts.json"])
    assert proof["restored"] is True
    assert proof["callers_unchanged"] is True
    assert proof["source"] == json.loads(archive["source.json"])
    assert proof["caller_sha256"] == {
        name: hashlib.sha256(archive[member]).hexdigest()
        for name, member in [
            ("tests/test_manager_ingress_boundaries.py", "candidate-tests.py"),
            ("docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py", "replay-source.py"),
        ]
    }
    receipts = proof["receipts"]
    assert len(receipts) == 8
    assert {receipt["node"] for receipt in receipts} == set(NODE_FAILURES)
    for node in NODE_FAILURES:
        pair = [receipt for receipt in receipts if receipt["node"] == node]
        assert [receipt["phase"] for receipt in pair] == ["red", "green"]
        red, green = pair
        assert red["source_sha256"] != proof["source"]["sha256"]
        assert green["source_sha256"] == proof["source"]["sha256"]
        for receipt, status, exit_code in [(red, "failed", 1), (green, "passed", 0)]:
            assert receipt["valid"] is True
            assert receipt["exit"] == exit_code
            xml = archive[f"mutations/{receipt['mutation']}-{receipt['phase']}.xml"]
            assert outcomes(xml) == {f"tests.test_manager_ingress_boundaries::{node}": status}
            if status == "failed":
                failure = ET.fromstring(xml).find(".//testcase/failure")
                assert failure.get("message", "").splitlines()[:1] == [NODE_FAILURES[node]]
