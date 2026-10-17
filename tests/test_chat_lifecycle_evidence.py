"""Verify the original eight-test chunk's complete suites from retained raw evidence."""

import hashlib
import json
import tarfile
from collections import Counter
from pathlib import Path, PurePosixPath
from xml.etree.ElementTree import SubElement

import pytest
from defusedxml import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/issue-1750-chat-lifecycle"
ADDED_NODES = {
    "tests.test_chat_chain_lifecycle::" + name
    for name in (
        "test_build_chain_unexpected_import_error_fails_closed",
        "test_build_chain_constructor_failure_closes_connection",
        "test_invoke_only_chain_preserves_context_payload_and_cleanup[empty]",
        "test_invoke_only_chain_preserves_context_payload_and_cleanup[populated]",
        "test_chain_without_executor_fails_and_closes_connection",
        "test_cleanup_error_does_not_replace_successful_chain_payload",
        "test_missing_filing_id_returns_400_and_closes_connection",
        "test_coroutine_chain_answer_is_awaited_before_normalization",
    )
}


@pytest.fixture(scope="module")
def archive():
    raw = (EVIDENCE / "validation.tar.gz").read_bytes()
    binding = json.loads((EVIDENCE / "archive.json").read_text())
    assert (
        hashlib.sha256(raw).hexdigest()
        == binding["archive_sha256"]
        == "0a7dffe103f538990cc2186436701535622d2a9ac4d91ec08c2eefdef2e41dd6"
    )
    with tarfile.open(EVIDENCE / "validation.tar.gz") as bundle:
        members = bundle.getmembers()
        assert all(member.isfile() for member in members)
        assert len({member.name for member in members}) == len(members)
        files = {member.name: bundle.extractfile(member).read() for member in members}
    manifest = json.loads(files.pop("manifest.json"))
    assert len(files) == binding["members_validated"] == 82
    assert files.keys() == manifest.keys()
    for name, data in files.items():
        assert len(data) == manifest[name]["bytes"], name
        assert hashlib.sha256(data).hexdigest() == manifest[name]["sha256"], name
    assert set(binding["bindings"]) == {
        "bound-inputs/api/chat.py",
        "bound-inputs/tests/test_chat_chain_lifecycle.py",
        "bound-inputs/docs/evidence/issue-1750-chat-lifecycle/replay.py",
    }
    for name, expected in binding["bindings"].items():
        assert manifest[name] == expected, name
        current = ROOT / name.removeprefix("bound-inputs/")
        if name == "bound-inputs/api/chat.py":
            # This archive proves a historical run, not an immutable product API.
            # The original lifecycle tests below remain byte-bound to the
            # archive and execute the current API in the normal test suite.
            current = EVIDENCE / "historical-api-chat.py.txt"
        if current.name == "replay.py":
            current = EVIDENCE / "historical-replay.py.txt"
        assert files[name] == current.read_bytes(), name
    successor = json.loads((EVIDENCE / "current-bindings.json").read_text())
    assert (
        successor["historical_driver_sha256"]
        == hashlib.sha256((EVIDENCE / "historical-replay.py.txt").read_bytes()).hexdigest()
    )
    assert (
        successor["current_driver_sha256"]
        == hashlib.sha256((EVIDENCE / "chat_lifecycle_replay.py").read_bytes()).hexdigest()
    )
    return files


def outcomes(raw):
    root = ET.fromstring(raw)
    suites = list(root.iter("testsuite"))
    assert len(suites) == 1, "Expected one suite"
    suite = suites[0]
    cases = list(root.iter("testcase"))
    assert cases and cases == list(suite), "Expected direct, counted test cases"
    result = {}
    counts = Counter(tests=len(cases), failures=0, errors=0, skipped=0)
    for case in cases:
        assert case.get("classname") and case.get("name"), "Missing case identity"
        node = f"{case.get('classname')}::{case.get('name')}"
        assert node not in result, f"Duplicate case: {node}"
        statuses = [child.tag for child in case if child.tag in {"failure", "error", "skipped"}]
        assert len(statuses) <= 1, f"Ambiguous outcome: {node}"
        assert "error" not in statuses, f"Test error: {node}"
        status = statuses[0] if statuses else "pass"
        result[node] = status
        if status != "pass":
            counts[{"failure": "failures", "skipped": "skipped"}[status]] += 1
    assert {key: suite.get(key) for key in counts} == {
        key: str(value) for key, value in counts.items()
    }, "Wrong suite counts"
    return result


def validate_full_suites(files, comparison):
    assert comparison["base_revision"] == "369ea9c399dbbd5b145d71e36cfb6862251a8a6b"
    baseline = outcomes(files["baseline/junit.xml"])
    candidate = outcomes(files["candidate/junit.xml"])
    assert baseline.keys() <= candidate.keys(), "Removed old nodes"
    assert {node: candidate[node] for node in baseline} == baseline, "Changed old outcomes"
    added = candidate.keys() - baseline.keys()
    assert added == ADDED_NODES == set(comparison["added_nodes"]), "Wrong added nodes"
    assert all(candidate[node] == "pass" for node in added), "New case did not pass"
    assert comparison["old_outcome_differences"] == []
    commands = []
    for phase, results, passed in (("baseline", baseline, 2093), ("candidate", candidate, 2101)):
        assert (
            Counter(results.values())
            == comparison[f"{phase}_outcomes"]
            == {"pass": passed, "failure": 24, "skipped": 21}
        ), "Wrong outcome totals"
        assert all(
            node.startswith("tests.test_ui_auth::")
            for node, status in results.items()
            if status == "failure"
        ), "Unexpected failing module"
        command = json.loads(files[f"{phase}/command.json"])
        assert command["exit"] == 1, "Full-suite exit must remain 1"
        argv = command["argv"]
        assert argv[1:6] == ["-m", "pytest", "tests", "-q", "--cov"], "Wrong suite command"
        assert len(argv) == 8
        for arg, prefix, filename in zip(
            argv[6:],
            ("--cov-report=json:", "--junitxml="),
            ("coverage.json", "junit.xml"),
            strict=True,
        ):
            assert arg.startswith(prefix), "Missing raw report argument"
            path = PurePosixPath(arg.removeprefix(prefix))
            assert path.parts[-2:] == (phase, filename), "Wrong report binding"
        assert PurePosixPath(command["cwd"]).is_absolute(), "Missing capture cwd"
        assert files[f"{phase}/console.txt"], "Missing raw console"
        commands.append(command)
    assert commands[0]["argv"][0] == commands[1]["argv"][0], "Different interpreters"
    assert commands[0]["cwd"] == commands[1]["cwd"], "Different suite checkouts"


def test_chat_archived_full_suites_preserve_every_old_outcome(archive):
    comparison = json.loads((EVIDENCE / "comparison.json").read_text())
    validate_full_suites(archive, comparison)


@pytest.mark.parametrize(
    "defect,message",
    [
        ("swap-old-outcomes", "Changed old outcomes"),
        ("remove-old-node", "Removed old nodes"),
        ("rename-new-node", "Wrong added nodes"),
        ("new-case-skipped", "New case did not pass"),
        ("exit-zero", "Full-suite exit must remain 1"),
        ("focused-command", "Wrong suite command"),
        ("wrong-report", "Wrong report binding"),
        ("different-checkout", "Different suite checkouts"),
    ],
)
def test_chat_full_suite_comparison_rejects_tampered_candidate(archive, defect, message):
    files = dict(archive)
    root = ET.fromstring(files["candidate/junit.xml"])
    suite = root.find("testsuite")
    if defect == "swap-old-outcomes":
        # Keep aggregate counts identical while changing two existing node outcomes.
        failed = next(case for case in suite if case.find("failure") is not None)
        passed = next(
            case
            for case in suite
            if len(case) == 0 and case.get("classname") != "tests.test_chat_chain_lifecycle"
        )
        failure = failed.find("failure")
        failed.remove(failure)
        passed.append(failure)
    elif defect == "remove-old-node":
        suite.remove(suite[0])
        suite.set("tests", str(int(suite.get("tests")) - 1))
    elif defect in {"rename-new-node", "new-case-skipped"}:
        case = next(
            case for case in suite if case.get("classname") == "tests.test_chat_chain_lifecycle"
        )
        if defect == "rename-new-node":
            case.set("name", "unrelated")
        else:
            SubElement(case, "skipped")
            suite.set("skipped", str(int(suite.get("skipped")) + 1))
    else:
        command = json.loads(files["candidate/command.json"])
        if defect == "exit-zero":
            command["exit"] = 0
        elif defect == "focused-command":
            command["argv"][3] = "tests/test_chat_chain_lifecycle.py"
        elif defect == "wrong-report":
            command["argv"][-1] = command["argv"][-1].replace("candidate/", "baseline/")
        else:
            command["cwd"] += "/different"
        files["candidate/command.json"] = json.dumps(command).encode()
    files["candidate/junit.xml"] = ET.tostring(root)
    comparison = json.loads((EVIDENCE / "comparison.json").read_text())
    with pytest.raises(AssertionError, match=message):
        validate_full_suites(files, comparison)


@pytest.mark.parametrize(
    "defect,message",
    [
        ("duplicate", "Duplicate case"),
        ("missing-name", "Missing case identity"),
        ("missing-classname", "Missing case identity"),
        ("error", "Test error"),
        ("ambiguous", "Ambiguous outcome"),
        ("wrong-count", "Wrong suite counts"),
        ("detached", "Expected direct, counted test cases"),
    ],
)
def test_chat_full_suite_parser_rejects_malformed_junit(archive, defect, message):
    root = ET.fromstring(archive["candidate/junit.xml"])
    suite = root.find("testsuite")
    case = suite[0]
    if defect == "duplicate":
        suite.append(ET.fromstring(ET.tostring(case)))
    elif defect.startswith("missing-"):
        del case.attrib[defect.removeprefix("missing-")]
    elif defect == "error":
        SubElement(case, "error")
    elif defect == "ambiguous":
        SubElement(case, "failure")
        SubElement(case, "skipped")
    elif defect == "wrong-count":
        suite.set("tests", "0")
    else:
        suite.remove(case)
        root.append(case)
    with pytest.raises(AssertionError, match=message):
        outcomes(ET.tostring(root))
