"""Prove ten dashboard error fallbacks using isolated, restored production mutations."""

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

SOURCE = "ui/dashboard.py"
TEST = "tests/test_dashboard_loader_failures.py"
SQL = ["load_filing_timeline", "load_latest_holdings_snapshot", "load_top_deltas"]
DOMAIN = [
    "load_manager_activism_filings",
    "load_manager_activism_events",
    "load_manager_activism_timeline",
    "load_active_campaigns_summary",
    "load_manager_conviction_scores",
    "load_manager_crowded_trades",
    "load_manager_contrarian_signals",
]


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate_junit(path, name, phase, returncode):
    """Require the selected assertion outcome, never a different failing test."""
    require(phase in {"red", "green"}, "Unknown phase")
    require(returncode == (1 if phase == "red" else 0), "Wrong phase exit")
    root = ET.parse(path).getroot()
    suites = list(root.iter("testsuite"))
    require(len(suites) == 1, "Expected one suite")
    suite = suites[0]
    require(
        {key: suite.get(key) for key in ("tests", "failures", "errors", "skipped")}
        == {"tests": "1", "failures": str(int(phase == "red")), "errors": "0", "skipped": "0"},
        "Wrong suite outcomes",
    )
    cases = list(root.iter("testcase"))
    require(
        len(cases) == 1
        and cases[0] in list(suite)
        and cases[0].get("name") == name
        and cases[0].get("classname") == "tests.test_dashboard_loader_failures",
        "Wrong test identity",
    )
    case = cases[0]
    outcomes = [node for node in root.iter() if node.tag in {"failure", "error", "skipped"}]
    require(
        [node.tag for node in outcomes] == (["failure"] if phase == "red" else [])
        and all(node in list(case) for node in outcomes),
        "Error, skip or misplaced outcome is not proof",
    )
    if phase == "red":
        require(
            "assert result.empty" in (outcomes[0].text or ""),
            "RED was not the intended empty-frame assertion",
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    caller = {name: digest((root / name).read_bytes()) for name in (SOURCE, TEST)}
    archive = subprocess.check_output(["git", "-C", str(root), "archive", "HEAD"], timeout=60)
    controls = []
    with tempfile.TemporaryDirectory(prefix="dashboard-fallback-proof-") as tmp:
        tree = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(tree, filter="data")
        (tree / TEST).write_bytes((root / TEST).read_bytes())
        source = tree / SOURCE
        original = source.read_bytes()
        require(digest(original) == caller[SOURCE], "Committed source differs from checkout")
        lines = original.decode().splitlines(keepends=True)
        for function in SQL + DOMAIN:
            node = next(
                n
                for n in ast.parse(original).body
                if isinstance(n, ast.FunctionDef) and n.name == function
            )
            handlers = [n for n in ast.walk(node) if isinstance(n, ast.ExceptHandler)]
            require(len(handlers) == 1, function)
            fallback = handlers[0].body[0]
            require(isinstance(fallback, ast.Assign), "Expected one fallback assignment")
            replacement = (
                'df = pd.DataFrame({"unexpected": [1]})'
                if function in SQL
                else 'return pd.DataFrame({"unexpected": [1]})'
            )
            mutant = (
                "".join(lines[: fallback.lineno - 1])
                + " " * fallback.col_offset
                + replacement
                + "\n"
                + "".join(lines[fallback.end_lineno :])
            ).encode()
            (output / (function + "-original.py")).write_bytes(original)
            (output / (function + "-mutant.py")).write_bytes(mutant)
            name = (
                (
                    "test_sql_loader_returns_typed_empty_frame_and_closes_missing_schema"
                    if function in SQL
                    else "test_domain_loader_returns_empty_and_closes_after_query_error"
                )
                + "["
                + function
                + "]"
            )
            control = {
                "node": TEST + "::" + name,
                "original_sha256": digest(original),
                "mutant_sha256": digest(mutant),
            }
            for phase, data in (("red", mutant), ("green", original)):
                try:
                    source.write_bytes(data)
                    xml = output / (function + "-" + phase + ".xml")
                    argv = [
                        sys.executable,
                        *(["-O"] if sys.flags.optimize else []),
                        "-m",
                        "pytest",
                        control["node"],
                        "-q",
                        "-o",
                        "addopts=",
                        "--junitxml=" + str(xml),
                    ]
                    with (output / (function + "-" + phase + ".log")).open("w") as log:
                        r = subprocess.run(
                            argv,
                            cwd=tree,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            timeout=120,
                            check=False,
                        )
                    receipt = {"argv": argv, "cwd": str(tree), "exit": r.returncode}
                    (output / (function + "-" + phase + ".json")).write_text(
                        json.dumps(receipt, indent=2)
                    )
                    validate_junit(xml, name, phase, r.returncode)
                    control[phase] = receipt
                finally:
                    source.write_bytes(original)
            control["restored_sha256"] = digest(source.read_bytes())
            require(control["restored_sha256"] == control["original_sha256"], "Restoration differs")
            controls.append(control)
    require(
        caller == {name: digest((root / name).read_bytes()) for name in caller}, "Caller changed"
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
    print("PASS: ten intended assertion RED1/restored GREEN0 pairs; caller bytes unchanged")


if __name__ == "__main__":
    main()
