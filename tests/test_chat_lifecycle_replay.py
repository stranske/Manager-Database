"""Chat lifecycle evidence must prove the exact case without overwriting prior proof."""

import importlib.util
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

DRIVER = (
    Path(__file__).resolve().parents[1]
    / "docs/evidence/issue-1750-chat-lifecycle/chat_lifecycle_replay.py"
)
SPEC = importlib.util.spec_from_file_location("chat_lifecycle_replay", DRIVER)
assert SPEC is not None and SPEC.loader is not None
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


def report(name, assertion, phase):
    root = Element("testsuites")
    suite = SubElement(
        root,
        "testsuite",
        tests="1",
        failures=str(int(phase == "red")),
        errors="0",
        skipped="0",
    )
    case = SubElement(suite, "testcase", classname="tests.test_chat_chain_lifecycle", name=name)
    if phase == "red":
        SubElement(case, "failure", message="AssertionError").text = (
            f">       {assertion}\nE       AssertionError"
        )
    return root, suite, case


def write_report(tmp_path, root):
    path = tmp_path / "phase.xml"
    path.write_bytes(tostring(root))
    return path


@pytest.mark.parametrize("case", replay.CASES, ids=[case[0] for case in replay.CASES])
@pytest.mark.parametrize("phase,code", [("red", 1), ("green", 0)])
def test_replay_accepts_exact_named_assertion_and_restored_phase(tmp_path, case, phase, code):
    name, assertion = case[-2:]
    root, _, _ = report(name, assertion, phase)
    replay.validate_junit(write_report(tmp_path, root), name, phase, code, assertion)


@pytest.mark.parametrize("attribute", ["name", "classname"])
@pytest.mark.parametrize("phase,code", [("red", 1), ("green", 0)])
def test_replay_rejects_malformed_case_identity(tmp_path, attribute, phase, code):
    name, assertion = replay.CASES[0][-2:]
    root, _, case = report(name, assertion, phase)
    case.set(attribute, "unrelated")
    with pytest.raises(ValueError, match="Wrong case identity"):
        replay.validate_junit(write_report(tmp_path, root), name, phase, code, assertion)


@pytest.mark.parametrize("outcome", ["error", "skipped"])
def test_replay_rejects_errors_and_skips_even_with_passing_suite_counts(tmp_path, outcome):
    name, assertion = replay.CASES[0][-2:]
    root, _, case = report(name, assertion, "green")
    SubElement(case, outcome)
    with pytest.raises(ValueError, match="Unexpected outcome"):
        replay.validate_junit(write_report(tmp_path, root), name, "green", 0, assertion)


@pytest.mark.parametrize("malformation", ["duplicate-case", "duplicate-suite", "detached-case"])
def test_replay_rejects_ambiguous_case_ownership(tmp_path, malformation):
    name, assertion = replay.CASES[0][-2:]
    root, suite, case = report(name, assertion, "red")
    if malformation == "duplicate-case":
        SubElement(suite, "testcase", attrib=dict(case.attrib))
    elif malformation == "duplicate-suite":
        SubElement(root, "testsuite", attrib=dict(suite.attrib))
    else:
        suite.remove(case)
        root.append(case)
    with pytest.raises(ValueError, match="Expected one counted case|Wrong case identity"):
        replay.validate_junit(write_report(tmp_path, root), name, "red", 1, assertion)


@pytest.mark.parametrize("traceback", ["", "assertion-in-context", "different-assertion"])
def test_replay_rejects_failure_without_intended_assertion(tmp_path, traceback):
    name, assertion = replay.CASES[0][-2:]
    root, _, case = report(name, assertion, "red")
    case[0].text = {
        "": "",
        "assertion-in-context": f"        {assertion}\n>       assert False\nE       AssertionError",
        "different-assertion": ">       assert False\nE       AssertionError",
    }[traceback]
    with pytest.raises(ValueError, match="Not the intended assertion"):
        replay.validate_junit(write_report(tmp_path, root), name, "red", 1, assertion)


@pytest.mark.parametrize("phase,code", [("red", 0), ("green", 1)])
def test_replay_rejects_wrong_process_exit(tmp_path, phase, code):
    name, assertion = replay.CASES[0][-2:]
    root, _, _ = report(name, assertion, phase)
    with pytest.raises(ValueError, match="Wrong process exit"):
        replay.validate_junit(write_report(tmp_path, root), name, phase, code, assertion)


@pytest.mark.parametrize("kind", ["directory", "file"])
def test_replay_refuses_existing_output_before_archive_or_mutation(tmp_path, monkeypatch, kind):
    output = tmp_path / "proof"
    if kind == "directory":
        output.mkdir()
        proof = output / "existing.xml"
    else:
        proof = output
    proof.write_bytes(b"existing proof must survive\n")
    root = DRIVER.parents[3]
    before = {name: (root / name).read_bytes() for name in (replay.SOURCE, replay.TEST)}

    def unexpected_child(*args, **kwargs):
        pytest.fail("Existing output must be rejected before archive creation or mutation")

    monkeypatch.setattr(replay.subprocess, "check_output", unexpected_child)
    monkeypatch.setattr(replay.subprocess, "run", unexpected_child)
    monkeypatch.setattr(replay.sys, "argv", [str(DRIVER), "--output", str(output)])
    with pytest.raises(FileExistsError):
        replay.main()

    assert proof.read_bytes() == b"existing proof must survive\n"
    if kind == "directory":
        assert list(output.iterdir()) == [proof]
    assert {name: (root / name).read_bytes() for name in before} == before


def test_archive_uses_selected_repository_with_poisoned_git_environment(tmp_path, monkeypatch):
    import io
    import os
    import subprocess
    import tarfile

    clean_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    for name in ("intended", "foreign"):
        tree = tmp_path / name
        tree.mkdir()
        subprocess.run(["git", "init", "-q", str(tree)], env=clean_env, check=True)
        (tree / "identity.txt").write_text(name)
        subprocess.run(["git", "-C", str(tree), "add", "."], env=clean_env, check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(tree),
                "-c",
                "user.name=Proof",
                "-c",
                "user.email=proof@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            env=clean_env,
            check=True,
        )
    foreign = tmp_path / "foreign"
    for key, value in {
        "GIT_DIR": str(foreign / ".git"),
        "GIT_WORK_TREE": str(foreign),
        "GIT_INDEX_FILE": str(foreign / ".git/index"),
        "GIT_OBJECT_DIRECTORY": str(foreign / ".git/objects"),
    }.items():
        monkeypatch.setenv(key, value)
    with tarfile.open(
        fileobj=io.BytesIO(replay.archive_current_tree(tmp_path / "intended"))
    ) as archive:
        assert archive.extractfile("identity.txt").read() == b"intended"
