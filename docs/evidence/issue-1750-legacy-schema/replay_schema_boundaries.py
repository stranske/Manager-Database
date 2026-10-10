"""Replay four real schema regressions; always restore the production bytes."""

import argparse
import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "api/managers.py"
TEST = "tests/test_manager_legacy_schema.py"
CASES = [
    (
        "json-default",
        "test_universe_upgrade_adds_missing_identifiers_and_native_json_defaults",
        "ALTER TABLE managers ADD COLUMN jurisdictions TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE managers ADD COLUMN jurisdictions TEXT NOT NULL DEFAULT '[1]'",
    ),
    (
        "created-backfill",
        "test_universe_upgrade_backfills_created_at_without_rewriting_updated_at",
        "UPDATE managers SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL",
        "SELECT 1",
    ),
    (
        "updated-backfill",
        "test_universe_upgrade_backfills_updated_at_without_rewriting_created_at",
        "UPDATE managers SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL",
        "SELECT 1",
    ),
    (
        "unique-index",
        "test_universe_upgrade_is_idempotent_and_enforces_cik_uniqueness",
        '        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique ON managers(cik)")\n',
        '        conn.execute("SELECT 1")\n',
    ),
]
EXPECTED_FAILURES = {
    "json-default": "AssertionError: legacy JSON defaults changed",
    "created-backfill": "AssertionError: legacy created_at backfill missing",
    "updated-backfill": "AssertionError: legacy updated_at backfill missing",
    "unique-index": "AssertionError: legacy CIK uniqueness index missing",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def run_case(output, name, node, phase):
    junit = output / f"{name}-{phase}.xml"
    argv = [
        sys.executable,
        "-m",
        "pytest",
        f"{TEST}::{node}",
        "-q",
        "-o",
        "addopts=",
        f"--junitxml={junit}",
    ]
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=90)
    (output / f"{name}-{phase}.txt").write_text(result.stdout + result.stderr)
    cases = ET.parse(junit).getroot().findall(".//testcase")
    failed = [case for case in cases if case.find("failure") is not None]
    bad = [
        case for case in cases if case.find("error") is not None or case.find("skipped") is not None
    ]
    expected = 1 if phase == "red" else 0
    valid = (
        result.returncode == expected
        and len(cases) == 1
        and cases[0].get("name") == node
        and not bad
        and len(failed) == expected
    )
    if phase == "red" and valid:
        message = failed[0].find("failure").get("message", "")
        valid = message.splitlines()[:1] == [EXPECTED_FAILURES[name]]
    return {
        "mutation": name,
        "phase": phase,
        "node": node,
        "argv": argv,
        "cwd": str(ROOT),
        "exit": result.returncode,
        "valid": valid,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    original = SOURCE.read_bytes()
    binding = json.loads((Path(__file__).parent / "source.json").read_text())
    if digest(original) != binding["sha256"]:
        raise RuntimeError("Source differs from reviewed baseline; refusing mutation")
    for name, _, before, _ in CASES:
        if original.decode().count(before) != 1:
            raise RuntimeError(f"Expected exactly one mutation site: {name}")
    caller_hashes = {
        str(path.relative_to(ROOT)): digest(path.read_bytes())
        for path in [ROOT / TEST, Path(__file__).resolve()]
    }
    receipts = []
    try:
        for name, node, before, after in CASES:
            text = original.decode()
            if text.count(before) != 1:
                raise RuntimeError(f"Expected exactly one mutation site: {name}")
            changed = text.replace(before, after).encode()
            try:
                SOURCE.write_bytes(changed)
                red = run_case(args.output, name, node, "red")
                red["source_sha256"] = digest(changed)
                receipts.append(red)
            finally:
                SOURCE.write_bytes(original)
            if SOURCE.read_bytes() != original:
                raise RuntimeError("Production restoration failed")
            green = run_case(args.output, name, node, "green")
            green["source_sha256"] = digest(SOURCE.read_bytes())
            receipts.append(green)
            if not red["valid"] or not green["valid"]:
                raise RuntimeError(f"Invalid RED/GREEN pair: {name}")
    finally:
        SOURCE.write_bytes(original)
        (args.output / "receipts.json").write_text(
            json.dumps(
                {
                    "source": binding,
                    "restored": SOURCE.read_bytes() == original,
                    "caller_sha256": caller_hashes,
                    "callers_unchanged": all(
                        digest((ROOT / path).read_bytes()) == value
                        for path, value in caller_hashes.items()
                    ),
                    "receipts": receipts,
                },
                indent=2,
            )
            + "\n"
        )
    print("Four production mutations: RED exit 1 / byte-identical restoration GREEN exit 0")


if __name__ == "__main__":
    main()
