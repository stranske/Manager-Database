"""Prove six manager database-adapter boundaries using isolated production mutations."""

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
TEST = "tests/test_manager_dialect_boundaries.py"
CASES = [
    (
        "test_postgres_array_filters_keep_values_in_parameters",
        "_manager_filter_clauses",
        'clauses.append("%s = ANY(jurisdictions)")',
        'clauses.append("%s = ANY(tags)")',
    ),
    (
        "test_postgres_universe_upsert_preserves_native_array_and_conflict_update",
        "_upsert_universe_record",
        "(name, cik, jurisdiction, jurisdictions),",
        "(name, cik, jurisdiction, json.dumps(jurisdictions)),",
    ),
    (
        "test_postgres_insert_missing_returned_identity_is_zero",
        "_insert_manager",
        "        return 0",
        "        return 1",
    ),
    (
        "test_postgres_registry_update_uses_jsonb_cast_and_serialized_parameter",
        "_update_manager",
        'set_clauses.append(f"{field} = {placeholder}::jsonb")',
        'set_clauses.append(f"{field} = {placeholder}")',
    ),
    (
        "test_postgres_conflict_update_preserves_old_flags_with_jsonb_cast",
        "_update_manager",
        'set_clauses.append(f"quality_flags = {placeholder}::jsonb")',
        'set_clauses.append(f"quality_flags = {placeholder}")',
    ),
    (
        "test_count_missing_aggregate_row_is_zero_without_cache",
        "_count_managers",
        "        return 0",
        "        return 1",
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
    print("PASS: six named RED1/restored GREEN0 pairs; caller identities unchanged")


if __name__ == "__main__":
    main()
