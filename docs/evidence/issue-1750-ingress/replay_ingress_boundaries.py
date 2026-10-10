"""Replay four real ingress regressions; always restore the production bytes."""

import argparse
import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "api/managers.py"
TEST = "tests/test_manager_ingress_boundaries.py"
CASES = [
    (
        "invalid-limit",
        "test_invalid_bulk_limit_uses_safe_default_and_warns",
        '        logger.warning("Invalid BULK_IMPORT_MAX_BYTES value: %s", raw_value)\n        return DEFAULT_BULK_IMPORT_MAX_BYTES',
        '        logger.warning("Invalid BULK_IMPORT_MAX_BYTES value: %s", raw_value)\n        return 1',
    ),
    (
        "empty-csv",
        "test_empty_csv_reports_missing_name_header_before_storage",
        '    if reader.fieldnames is None:\n        return [], ["name"]',
        "    if reader.fieldnames is None:\n        return [], []",
    ),
    (
        "invalid-cik",
        "test_invalid_patch_cik_is_rejected_before_storage",
        'def _validate_manager_update_payload(payload: ManagerUpdate) -> list[dict[str, str]]:\n    """Apply validation checks for partial updates."""\n    errors: list[dict[str, str]] = []\n    provided_fields = payload.model_dump(exclude_unset=True)\n    if not provided_fields:\n        errors.append({"field": "body", "message": "At least one field must be provided."})\n        return errors\n    if payload.name is not None and not payload.name.strip():\n        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})\n    if (\n        payload.cik is not None\n        and payload.cik.strip()\n        and not CIK_PATTERN.match(payload.cik.strip())\n    ):\n        errors.append({"field": "cik", "message": "CIK must be a 10-digit zero-padded string."})\n    return errors\n\n',
        'def _validate_manager_update_payload(payload: ManagerUpdate) -> list[dict[str, str]]:\n    """Apply validation checks for partial updates."""\n    errors: list[dict[str, str]] = []\n    provided_fields = payload.model_dump(exclude_unset=True)\n    if not provided_fields:\n        errors.append({"field": "body", "message": "At least one field must be provided."})\n        return errors\n    if payload.name is not None and not payload.name.strip():\n        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})\n    if (\n        payload.cik is not None\n        and payload.cik.strip()\n        and False\n    ):\n        errors.append({"field": "cik", "message": "CIK must be a 10-digit zero-padded string."})\n    return errors\n\n',
    ),
    (
        "invalid-length",
        "test_invalid_content_length_still_bounds_actual_bytes_before_storage",
        "        except ValueError:\n            declared_length = None",
        '        except ValueError:\n            return _bulk_request_error("body", "Invalid Content-Length.")',
    ),
]
EXPECTED_FAILURES = {
    "invalid-limit": "AssertionError: invalid limit lost safe default",
    "empty-csv": "AssertionError: empty CSV lost missing-header diagnosis",
    "invalid-cik": "AssertionError: invalid request reached database",
    "invalid-length": "AssertionError: actual bytes were not bounded",
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
