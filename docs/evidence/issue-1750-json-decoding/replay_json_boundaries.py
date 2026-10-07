"""Prove six legacy-decoding tests using isolated production mutations."""

import argparse
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from defusedxml import ElementTree as ET

SOURCE = "api/managers.py"
TEST = "tests/test_manager_legacy_decoding.py"
CASES = [
    (
        "test_array_decoder_preserves_native_values_and_rejects_wrong_json_shapes",
        "if isinstance(raw, list):\n        return [str(item) for item in raw]",
        "if isinstance(raw, list):\n        return []",
    ),
    (
        "test_array_decoder_trims_semicolon_legacy_values_and_discards_empty_parts",
        'return [part.strip() for part in text.split(";") if part.strip()]',
        'return [part.strip() for part in text.split(";")]',
    ),
    (
        "test_array_decoder_trims_comma_legacy_values_and_discards_empty_parts",
        'return [part.strip() for part in text.split(",") if part.strip()]',
        'return [part.strip() for part in text.split(",")]',
    ),
    (
        "test_registry_decoder_converts_native_keys_and_values_without_accepting_invalid_shapes",
        "if isinstance(raw, dict):\n        return {str(key): str(value) for key, value in raw.items()}",
        "if isinstance(raw, dict):\n        return {}",
    ),
    (
        "test_quality_flags_decoder_retains_only_objects_and_rejects_invalid_shapes",
        "if isinstance(raw, list):\n        return [item for item in raw if isinstance(item, dict)]",
        "if isinstance(raw, list):\n        return raw",
    ),
    (
        "test_legacy_manager_row_keeps_timestamps_separate_from_quality_flags",
        "created_at_raw = row[9] if len(row) > 10 else row[8]",
        "created_at_raw = row[9]",
    ),
]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    caller = {name: digest((root / name).read_bytes()) for name in (SOURCE, TEST)}
    archive = subprocess.check_output(["git", "-C", str(root), "archive", "HEAD"])
    controls = []
    with tempfile.TemporaryDirectory(prefix="manager-decoding-proof-") as tmp:
        tree = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(tree, filter="data")
        (tree / TEST).write_bytes((root / TEST).read_bytes())
        source = tree / SOURCE
        original = source.read_bytes()
        if digest(original) != caller[SOURCE]:
            raise ValueError("Archive source differs from checkout; commit source changes first")
        for name, before, after in CASES:
            assert original.decode().count(before) == 1, name
            mutant = original.decode().replace(before, after, 1).encode()
            control = {
                "node": f"{TEST}::{name}",
                "original_sha256": digest(original),
                "mutated_sha256": digest(mutant),
            }
            for phase, data, expected in (("red", mutant, 1), ("green", original, 0)):
                try:
                    source.write_bytes(data)
                    xml = output / f"{name}-{phase}.xml"
                    argv = [
                        sys.executable,
                        "-m",
                        "pytest",
                        control["node"],
                        "-q",
                        "-o",
                        "addopts=",
                        f"--junitxml={xml}",
                    ]
                    with (output / f"{name}-{phase}.log").open("w") as log:
                        r = subprocess.run(
                            argv, cwd=tree, stdout=log, stderr=subprocess.STDOUT, timeout=120
                        )
                    receipt = {"argv": argv, "cwd": str(tree), "exit": r.returncode}
                    (output / f"{name}-{phase}.json").write_text(json.dumps(receipt, indent=2))
                    cases = list(ET.parse(xml).getroot().iter("testcase"))
                    assert r.returncode == expected, (name, phase, r.returncode)
                    assert len(cases) == 1 and cases[0].get("name") == name
                    assert not list(cases[0].iter("error")) and not list(cases[0].iter("skipped"))
                    assert bool(list(cases[0].iter("failure"))) is (phase == "red")
                    control[phase] = receipt
                finally:
                    source.write_bytes(original)
            control["restored_sha256"] = digest(source.read_bytes())
            assert control["restored_sha256"] == control["original_sha256"]
            controls.append(control)
    assert caller == {name: digest((root / name).read_bytes()) for name in caller}
    (output / "controls.json").write_text(
        json.dumps({"caller_identity": caller, "controls": controls}, indent=2)
    )
    print("PASS: six named RED1/restored GREEN0 pairs; caller identities unchanged")


if __name__ == "__main__":
    main()
