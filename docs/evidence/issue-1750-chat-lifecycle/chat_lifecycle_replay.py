"""Replay eight named assertion failures in a disposable archive, restoring exact bytes."""

import argparse
import ast
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from defusedxml import ElementTree as ET

SOURCE = "api/chat.py"
TEST = "tests/test_chat_chain_lifecycle.py"
CASES = [
    (
        "import-error",
        "_build_chain",
        "raise ChainUnavailableError(f\"Chat chain '{chain_name}' failed during import\") from exc",
        "return None",
        "test_build_chain_unexpected_import_error_fails_closed",
        "assert isinstance(error, chat.ChainUnavailableError)",
    ),
    (
        "constructor-close",
        "_build_chain",
        "db_conn.close()",
        "pass",
        "test_build_chain_constructor_failure_closes_connection",
        "assert connection.closes == 1",
    ),
    (
        "invoke-empty",
        "_run_chain",
        'result = chain.invoke({"question": question, "context": normalized_context or {}})',
        'result = chain.invoke({"question": question, "context": None})',
        "test_invoke_only_chain_preserves_context_payload_and_cleanup[empty]",
        'assert calls == [{"question": "question", "context": context}]',
    ),
    (
        "invoke-populated",
        "_run_chain",
        'result = chain.invoke({"question": question, "context": normalized_context or {}})',
        'result = chain.invoke({"question": question, "context": None})',
        "test_invoke_only_chain_preserves_context_payload_and_cleanup[populated]",
        'assert calls == [{"question": "question", "context": context}]',
    ),
    (
        "no-executor",
        "_run_chain",
        "raise RuntimeError(f\"Chain '{chain_name}' does not expose run/invoke\")",
        'result = {"answer": "unexpected"}',
        "test_chain_without_executor_fails_and_closes_connection",
        "assert isinstance(error, RuntimeError)",
    ),
    (
        "cleanup",
        "_run_chain",
        'if close is not None and hasattr(close, "close"):',
        'if False and close is not None and hasattr(close, "close"):',
        "test_cleanup_error_does_not_replace_successful_chain_payload",
        "assert context_connection.closes == chain_connection.closes == 1",
    ),
    (
        "missing-filing",
        "_run_chain",
        'raise TypeError("missing filing_id")',
        "filing_id = 0",
        "test_missing_filing_id_returns_400_and_closes_connection",
        "assert isinstance(error, chat.HTTPException)",
    ),
    (
        "coroutine",
        "_run_chain",
        "result = await result",
        'result = await result\n        result["answer"] = "mutated"',
        "test_coroutine_chain_answer_is_awaited_before_normalization",
        'assert result == ("async answer", [], None, "https://trace.invalid/17")',
    ),
]


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate_junit(path, name, phase, code, assertion):
    require(phase in {"red", "green"}, "Unknown phase")
    require(code == (1 if phase == "red" else 0), "Wrong process exit")
    root = ET.parse(path).getroot()
    suites = list(root.iter("testsuite"))
    cases = list(root.iter("testcase"))
    require(len(suites) == 1 and len(cases) == 1, "Expected one counted case")
    suite, case = suites[0], cases[0]
    require(
        {k: suite.get(k) for k in ("tests", "failures", "errors", "skipped")}
        == {"tests": "1", "failures": str(int(phase == "red")), "errors": "0", "skipped": "0"},
        "Wrong suite counts",
    )
    require(
        case in list(suite)
        and case.get("name") == name
        and case.get("classname") == "tests.test_chat_chain_lifecycle",
        "Wrong case identity",
    )
    outcomes = [n for n in root.iter() if n.tag in {"failure", "error", "skipped"}]
    require(
        [n.tag for n in outcomes] == (["failure"] if phase == "red" else [])
        and all(n in list(case) for n in outcomes),
        "Unexpected outcome",
    )
    if phase == "red":
        require(
            any(
                line.startswith(">") and line[1:].strip() == assertion
                for line in (outcomes[0].text or "").splitlines()
            ),
            "Not the intended assertion",
        )


def archive_current_tree(root):
    child_env = os.environ.copy()
    for name in (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    ):
        child_env.pop(name, None)
    return subprocess.check_output(
        ["git", "-C", str(root), "archive", "HEAD"], env=child_env, timeout=60
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    identity = {n: digest((root / n).read_bytes()) for n in (SOURCE, TEST)}
    archive = archive_current_tree(root)
    controls = []
    with tempfile.TemporaryDirectory(prefix="chat-lifecycle-") as tmp:
        tree = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(tree, filter="data")
        (tree / TEST).write_bytes((root / TEST).read_bytes())
        source = tree / SOURCE
        original = source.read_bytes()
        require(digest(original) == identity[SOURCE], "Uncommitted production source")
        lines = original.decode().splitlines(keepends=True)
        for key, function, old, new, name, assertion in CASES:
            node = next(
                n
                for n in ast.parse(original).body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function
            )
            section = "".join(lines[node.lineno - 1 : node.end_lineno])
            require(section.count(old) == 1, "Mutation anchor is not unique")
            mutant = (
                "".join(lines[: node.lineno - 1])
                + section.replace(old, new, 1)
                + "".join(lines[node.end_lineno :])
            ).encode()
            ast.parse(mutant)
            (output / (key + "-original.py")).write_bytes(original)
            (output / (key + "-mutant.py")).write_bytes(mutant)
            control = {
                "node": TEST + "::" + name,
                "assertion": assertion,
                "original_sha256": digest(original),
                "mutant_sha256": digest(mutant),
            }
            for phase, data in [("red", mutant), ("green", original)]:
                try:
                    source.write_bytes(data)
                    require(source.read_bytes() == data, "Phase source mismatch")
                    xml = output / (key + "-" + phase + ".xml")
                    argv = [
                        sys.executable,
                        "-m",
                        "pytest",
                        control["node"],
                        "-q",
                        "-o",
                        "addopts=",
                        "--junitxml=" + str(xml),
                    ]
                    with (output / (key + "-" + phase + ".log")).open("w") as log:
                        result = subprocess.run(
                            argv,
                            cwd=tree,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            timeout=120,
                            check=False,
                        )
                    receipt = {"argv": argv, "cwd": str(tree), "exit": result.returncode}
                    (output / (key + "-" + phase + ".json")).write_text(
                        json.dumps(receipt, indent=2)
                    )
                    validate_junit(xml, name, phase, result.returncode, assertion)
                    control[phase] = receipt
                finally:
                    source.write_bytes(original)
            require(source.read_bytes() == original, "Restoration differs")
            control["restored_sha256"] = digest(source.read_bytes())
            controls.append(control)
    require(identity == {n: digest((root / n).read_bytes()) for n in identity}, "Caller changed")
    (output / "controls.json").write_text(
        json.dumps(
            {
                "caller_identity": identity,
                "driver_sha256": digest(Path(__file__).read_bytes()),
                "python_version": sys.version,
                "controls": controls,
            },
            indent=2,
        )
    )
    print("PASS: eight intended assertion RED1/restored GREEN0 pairs; caller unchanged")


if __name__ == "__main__":
    main()
