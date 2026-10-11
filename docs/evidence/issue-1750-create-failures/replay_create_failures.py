"""Prove four manager create/import boundaries using isolated production mutations."""

import argparse
import ast
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
TEST = "tests/test_manager_create_failure_boundaries.py"
CASES = [
    (
        "test_create_unrelated_schema_integrity_error_returns_503",
        "create_manager",
        'if "UNIQUE constraint failed: managers.cik" not in str(exc):',
        "if False:",
    ),
    (
        "test_create_non_cik_insert_failure_returns_503",
        "create_manager",
        "            raise\n        invalidate_cache_prefix",
        '            raise HTTPException(status_code=409, detail="unexpected") from exc\n        invalidate_cache_prefix',
    ),
    (
        "test_universe_import_write_failure_returns_503",
        "import_manager_universe",
        "_raise_db_unavailable(exc)",
        'raise HTTPException(status_code=409, detail="unexpected") from exc',
    ),
    (
        "test_create_missing_readback_returns_committed_payload",
        "create_manager",
        "if row is not None:\n        return _to_manager_response(row)",
        'if row is None:\n        raise HTTPException(status_code=404, detail="Manager not found")',
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
    with tempfile.TemporaryDirectory(prefix="manager-create-proof-") as tmp:
        tree = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(tree, filter="data")
        (tree / TEST).write_bytes((root / TEST).read_bytes())
        source = tree / SOURCE
        original = source.read_bytes()
        if digest(original) != caller[SOURCE]:
            raise ValueError("Archive source differs from checkout; commit source changes first")
        for name, function, before, after in CASES:
            node = next(
                n
                for n in ast.parse(original).body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function
            )
            lines = original.decode().splitlines(keepends=True)
            block = "".join(lines[node.lineno - 1 : node.end_lineno])
            assert block.count(before) == 1, name
            mutant = (
                "".join(lines[: node.lineno - 1])
                + block.replace(before, after, 1)
                + "".join(lines[node.end_lineno :])
            ).encode()
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
                        r = subprocess.run(  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
                            argv,
                            cwd=tree,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            timeout=120,
                            check=False,
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
        json.dumps(
            {
                "caller_identity": caller,
                "replay_sha256": digest(Path(__file__).read_bytes()),
                "python_version": sys.version,
                "controls": controls,
            },
            indent=2,
        )
    )
    print("PASS: four named RED1/restored GREEN0 pairs; caller identities unchanged")


if __name__ == "__main__":
    main()
