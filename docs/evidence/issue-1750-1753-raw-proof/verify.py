"""Read-only verification of retained manager outage acceptance artifacts.

Run with the repository's Python interpreter; no application imports are needed.
This verifies receipts, not a live database or the broader coverage target.
"""

import argparse
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

MUTATIONS = {
    "outage-status": 9,
    "no-close": 6,
    "cache-invalidation": 9,
    "private-detail": 9,
    "double-close": 6,
    "early-close": 6,
    "drop-retained-tags": 1,
}
EXPECTED_NODES = {
    "tests.test_manager_write_failures::"
    f"test_write_outage_returns_503_and_preserves_cache[{stage}-{operation}]"
    for stage in ("connect", "schema", "write")
    for operation in ("update", "tags", "delete")
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def junit(path):
    root = ET.parse(path).getroot()
    suites = list(root.iter("testsuite"))
    require(len(suites) == 1, f"{path.name}: expected one test suite")
    suite = suites[0]
    cases = list(suite.iter("testcase"))
    nodes = [f"{case.get('classname')}::{case.get('name')}" for case in cases]
    require(len(nodes) == len(set(nodes)), f"{path.name}: duplicate test nodes")
    counts = {
        "tests": len(cases),
        "failures": sum(case.find("failure") is not None for case in cases),
        "errors": sum(case.find("error") is not None for case in cases),
        "skipped": sum(case.find("skipped") is not None for case in cases),
    }
    for key, count in counts.items():
        require(int(suite.get(key, "-1")) == count, f"{path.name}: inconsistent {key}")
    failures = {
        node
        for node, case in zip(nodes, cases, strict=True)
        if case.find("failure") is not None or case.find("error") is not None
    }
    return counts, set(nodes), failures


def focused_receipt(path, failures):
    counts, nodes, failed_nodes = junit(path)
    require(nodes == EXPECTED_NODES, f"{path.name}: incomplete outage case matrix")
    require(
        counts == {"tests": 9, "failures": failures, "errors": 0, "skipped": 0},
        f"{path.name}: unexpected outcomes",
    )
    return failed_nodes


def verify(evidence, repository):
    manifest = json.loads((evidence / "manifest.json").read_text())
    hashes = manifest["files_sha256"]
    required_artifacts = (
        {f"1753-{name}-RED.{extension}" for name in MUTATIONS for extension in ("txt", "xml")}
        | {
            f"manager-{version}{suffix}"
            for version in ("baseline", "candidate")
            for suffix in (".log", ".xml", "-coverage.json")
        }
        | {"1753-restored-GREEN.txt", "1753-restored-GREEN.xml"}
    )
    require(required_artifacts <= hashes.keys(), "required captures are missing from hash index")
    for name, expected in hashes.items():
        path = (evidence / name).resolve()
        require(path.is_relative_to(evidence.resolve()), "artifact outside evidence directory")
        require(path.is_file(), f"{name}: missing or non-regular artifact")
        require(digest(path) == expected, f"{name}: SHA256 mismatch")
    for record in manifest["files"]:
        name = record["path"]
        require(name in hashes, f"{name}: missing from hash index")
        require(record["sha256"] == hashes[name], f"{name}: conflicting provenance hashes")
        require(bool(record["provenance"]), f"{name}: missing provenance")
    require(
        digest(repository / "api/managers.py") == manifest["production_sha256"],
        "current production source differs from accepted source",
    )
    require(
        digest(repository / "tests/test_manager_write_failures.py") == manifest["test_sha256"],
        "current outage tests differ from verified tests",
    )

    historical = {}
    for version in ("baseline", "candidate"):
        record = manifest["historical_full_suite"][version]
        counts, _, failures = junit(evidence / f"manager-{version}.xml")
        require(failures == set(record["failing_nodes"]), f"{version}: failing nodes differ")
        for key, count in counts.items():
            require(count == int(record["junit_counts"][key]), f"{version}: counts differ")
        coverage = json.loads((evidence / f"manager-{version}-coverage.json").read_text())
        require(coverage["totals"] == record["coverage_totals"], f"{version}: totals differ")
        require(sorted(coverage["files"]) == record["file_scope"], f"{version}: scope differs")
        historical[version] = (failures, coverage)
    baseline_failures, baseline = historical["baseline"]
    candidate_failures, candidate = historical["candidate"]
    require(baseline_failures == candidate_failures, "baseline/candidate failures differ")
    require(baseline["files"].keys() == candidate["files"].keys(), "coverage scopes differ")
    require(len(candidate["files"]) == 114, "unexpected coverage file count")
    for name, before in baseline["files"].items():
        after = candidate["files"][name]
        require(
            before["summary"]["num_statements"] == after["summary"]["num_statements"],
            f"{name}: coverage denominator differs",
        )

    mutations = {record["name"]: record for record in manifest["independent_actual_mutations"]}
    require(len(manifest["independent_actual_mutations"]) == 7, "duplicate or missing mutations")
    require(mutations.keys() == MUTATIONS.keys(), "unexpected mutation controls")
    for name, failures in MUTATIONS.items():
        require(
            mutations[name] == {"name": name, "tests": 9, "failures": failures, "exit": 1},
            f"{name}: inconsistent mutation record",
        )
        failed_nodes = focused_receipt(evidence / f"1753-{name}-RED.xml", failures)
        if failures == 6:
            require(
                failed_nodes == {node for node in EXPECTED_NODES if "[connect-" not in node},
                f"{name}: unexpected cleanup failures",
            )
        if name == "drop-retained-tags":
            require(
                failed_nodes == {node for node in EXPECTED_NODES if "[write-tags]" in node},
                "retained-tag control failed an unexpected case",
            )
    focused_receipt(evidence / "1753-restored-GREEN.xml", 0)
    restoration = manifest["restoration"]
    require(
        restoration
        == {
            "tests": 9,
            "failures": 0,
            "exit": 0,
            "production_sha256": manifest["production_sha256"],
        },
        "inconsistent source restoration record",
    )
    current = manifest["current_verification"]
    require(current["exit"] == 0, "current outage replay failed")
    require(current["production_sha256"] == manifest["production_sha256"], "replay source differs")
    require(current["test_sha256"] == manifest["test_sha256"], "replay tests differ")
    for key in ("console", "junit"):
        require(current[key] in hashes, f"current {key} capture is not hashed")
    focused_receipt(evidence / current["junit"], 0)
    return {
        "hashed_artifacts": len(hashes),
        "outage_cases_passed": 9,
        "semantic_controls_red": len(MUTATIONS),
        "unchanged_historical_failures": len(candidate_failures),
        "identical_coverage_file_scope": len(candidate["files"]),
        "historical_candidate_coverage_percent": candidate["totals"]["percent_covered"],
        "production_sha256": manifest["production_sha256"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    try:
        result = verify(args.evidence, args.repository)
    except (ValueError, KeyError, TypeError, AttributeError, OSError, ET.ParseError) as exc:
        parser.exit(1, f"Evidence verification failed: {exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
