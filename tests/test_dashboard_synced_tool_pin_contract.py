"""Exercise issue 1885's six-tool exception through the complete evidence validator."""

import json
import tomllib

import pytest

from tests import test_dashboard_read_failure_evidence as evidence

TOOLS = ("black", "ruff", "coverage", "mypy", "pytest-cov", "pytest")


@pytest.fixture
def current_inputs(tmp_path):
    manifest = json.loads(
        (evidence.EVIDENCE / "review-recovery-20261012/manifest.json").read_text()
    )
    for name in manifest["current_inputs"]:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((evidence.ROOT / name).read_bytes())
    return tmp_path, manifest


def replace_tool(project, tool, requirements):
    raw = project.read_text()
    dependencies = tomllib.loads(raw)["project"]["optional-dependencies"]["dev"]
    pin = next(value for value in dependencies if value.startswith(tool + "=="))
    original = json.dumps(pin) + ","
    assert raw.count(original) == 1
    project.write_text(raw.replace(original, "".join(json.dumps(v) + "," for v in requirements)))


def test_successor_exception_names_exactly_the_reviewed_six_tools():
    successor = json.loads(
        (evidence.ROOT / "docs/evidence/issue-1885/synced-tool-pin-successor.json").read_text()
    )
    assert successor["synced_dev_tools"] == list(TOOLS)
    assert evidence.SYNCED_DEV_TOOLS == TOOLS


@pytest.mark.parametrize("tools", [(tool,) for tool in TOOLS] + [TOOLS])
def test_full_validator_accepts_only_tool_version_bumps(current_inputs, tools):
    root, manifest = current_inputs
    project = root / "pyproject.toml"
    original = project.read_bytes()
    for tool in tools:
        replace_tool(project, tool, [tool + "==99.88.77"])
    assert project.read_bytes() != original
    evidence.validate_review_recovery_current_inputs(root, manifest)


@pytest.mark.parametrize("tool", TOOLS)
@pytest.mark.parametrize(
    "requirement",
    [
        "{tool}>=99.88.77",
        "{tool}==99.88.*",
        "{tool}[extra]==99.88.77",
        "{tool}==99.88.77; python_version >= '3.13'",
        "{tool} @ https://example.invalid/tool.whl",
        "{tool}==99.88.77rc1",
        "{tool}==99.88.77+local",
        "{tool}",
    ],
)
def test_full_validator_rejects_non_exact_requirements(current_inputs, tool, requirement):
    root, manifest = current_inputs
    replace_tool(root / "pyproject.toml", tool, [requirement.format(tool=tool)])
    with pytest.raises(AssertionError):
        evidence.validate_review_recovery_current_inputs(root, manifest)


@pytest.mark.parametrize("tool", TOOLS)
@pytest.mark.parametrize("requirements", [[], ["==99.88.77", "==99.88.78"]])
def test_full_validator_rejects_missing_and_duplicate_tools(current_inputs, tool, requirements):
    root, manifest = current_inputs
    replace_tool(root / "pyproject.toml", tool, [tool + pin for pin in requirements])
    with pytest.raises(AssertionError):
        evidence.validate_review_recovery_current_inputs(root, manifest)


@pytest.mark.parametrize(
    "before,after",
    [
        ('name = "manager-database"', 'name = "different-product"'),
        ('"pytest-asyncio"', '"pytest-asyncio==99.88.77"'),
        ('"httpx"', '"httpx==99.88.77"'),
        ('"streamlit==1.60.0"', '"streamlit==99.88.77"'),
        ('requires-python = ">=3.13"', 'requires-python = ">=3.14"'),
        ("warn_return_any = false", "warn_return_any = true"),
    ],
)
def test_six_pin_bumps_do_not_hide_unrelated_project_changes(current_inputs, before, after):
    root, manifest = current_inputs
    project = root / "pyproject.toml"
    for tool in TOOLS:
        replace_tool(project, tool, [tool + "==99.88.77"])
    evidence.validate_review_recovery_current_inputs(root, manifest)
    raw = project.read_text()
    assert raw.count(before) == 1
    project.write_text(raw.replace(before, after))
    with pytest.raises(AssertionError):
        evidence.validate_review_recovery_current_inputs(root, manifest)
