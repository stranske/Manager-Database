"""Replay eight named assertion failures in a disposable archive, restoring exact bytes."""

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

SOURCE = "api/chat.py"
TEST = "tests/test_chat_payload_boundaries.py"
CASES = [
    (
        "string",
        "_extract_chain_payload",
        "return result, [], None, None",
        'return "", [], None, None',
        "test_chain_string_answer_keeps_empty_metadata",
        'assert result == ("plain answer", [], None, None)',
    ),
    (
        "source-list",
        "_normalize_sources",
        'normalized.append({"description": str(source)})',
        'normalized.append({"description": ""})',
        "test_chain_source_list_preserves_dicts_and_describes_non_dicts",
        "assert result == [",
    ),
    (
        "unsupported-none",
        "_normalize_sources",
        "return []",
        'return [{"unexpected": 1}]',
        "test_chain_unsupported_source_container_stays_empty[none]",
        "assert result == []",
    ),
    (
        "unsupported-text",
        "_normalize_sources",
        "return []",
        'return [{"unexpected": 1}]',
        "test_chain_unsupported_source_container_stays_empty[text]",
        "assert result == []",
    ),
    (
        "unsupported-number",
        "_normalize_sources",
        "return []",
        'return [{"unexpected": 1}]',
        "test_chain_unsupported_source_container_stays_empty[number]",
        "assert result == []",
    ),
    (
        "unsupported-tuple",
        "_normalize_sources",
        "return []",
        'return [{"unexpected": 1}]',
        "test_chain_unsupported_source_container_stays_empty[tuple]",
        "assert result == []",
    ),
    (
        "object-response",
        "_extract_chain_payload",
        'answer = str(getattr(result, "answer", "") or getattr(result, "response", ""))',
        'answer = ""',
        "test_chain_object_response_fallback_preserves_all_metadata",
        "assert result == (",
    ),
    (
        "object-empty",
        "_extract_chain_payload",
        'sql = getattr(result, "sql", None)',
        'sql = getattr(result, "sql", "unexpected")',
        "test_chain_object_without_optional_attributes_has_empty_metadata",
        'assert result == ("", [], None, None)',
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
        and case.get("classname") == "tests.test_chat_payload_boundaries",
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    identity = {n: digest((root / n).read_bytes()) for n in (SOURCE, TEST)}
    archive = subprocess.check_output(["git", "-C", str(root), "archive", "HEAD"], timeout=60)
    controls = []
    with tempfile.TemporaryDirectory(prefix="chat-payload-") as tmp:
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
                if isinstance(n, ast.FunctionDef) and n.name == function
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
