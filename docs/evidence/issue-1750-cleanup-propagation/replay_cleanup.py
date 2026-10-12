"""Prove committed-import cleanup error propagation with an isolated production mutation."""

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
TEST = "tests/test_manager_bulk_cleanup_failure.py"
CASES = [
    (
        "test_bulk_import_reports_autocommit_restore_failure_after_commit",
        "bulk_import_managers",
        'logger.warning("Bulk import autocommit restoration failed: %s", cleanup_error)\n                    if db_error is None:\n                        db_error = cleanup_error',
        'logger.warning("Bulk import autocommit restoration failed: %s", cleanup_error)\n                    if db_error is None:\n                        pass',
    )
]


def require(condition, detail="Proof condition failed"):
    if not condition:
        raise ValueError(detail)


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
    with tempfile.TemporaryDirectory(prefix="manager-dialect-proof-") as tmp:
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
            require(block.count(before) == 1, name)
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
                        *(["-O"] if sys.flags.optimize else []),
                        "-m",
                        "pytest",
                        control["node"],
                        "-m",
                        "not slow",
                        "-q",
                        "-o",
                        "addopts=",
                        f"--junitxml={xml}",
                    ]
                    with (output / f"{name}-{phase}.log").open("w") as log:
                        r = subprocess.run(
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
                    require(r.returncode == expected, (name, phase, r.returncode))
                    require(
                        len(cases) == 1 and cases[0].get("name") == name, "Proof condition failed"
                    )
                    require(
                        not list(cases[0].iter("error")) and (not list(cases[0].iter("skipped"))),
                        "Proof condition failed",
                    )
                    require(
                        bool(list(cases[0].iter("failure"))) is (phase == "red"),
                        "Proof condition failed",
                    )
                    control[phase] = receipt
                finally:
                    source.write_bytes(original)
            control["restored_sha256"] = digest(source.read_bytes())
            require(
                control["restored_sha256"] == control["original_sha256"], "Proof condition failed"
            )
            controls.append(control)
    require(
        caller == {name: digest((root / name).read_bytes()) for name in caller},
        "Proof condition failed",
    )
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
    print("PASS: one named RED1/restored GREEN0 pair; caller identities unchanged")


if __name__ == "__main__":
    main()
