"""Keep repository-owned CI callers on the owner's tested Python standard."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
RUNTIME_KEYS = {
    "python-version",
    "python-versions",
    "primary-python-version",
    "coverage-python-version",
    "PYTHON_VERSION",
    "COVERAGE_REFERENCE_RUNTIME",
}
# Matrix axes that commonly feed actions/setup-python.
MATRIX_RUNTIME_KEYS = {"python", "python_version", "python-version"}
FROMJSON_MATRIX_RE = re.compile(
    r"""^\$\{\{\s*fromJSON\(\s*(['"])(?P<payload>.*)\1\s*\)\s*\}\}$""",
    re.DOTALL,
)
OPAQUE_MATRIX = object()
MATRIX_AXIS_RE = re.compile(r"""\bmatrix(?:\.([A-Za-z_][\w-]*)|\[\s*['"]([^'"]+)['"]\s*\])""")


def _referenced_matrix_axes(node: Any) -> set[str]:
    axes: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key in RUNTIME_KEYS and isinstance(value, str):
                for match in MATRIX_AXIS_RE.finditer(value):
                    axes.add(match.group(1) or match.group(2))
            axes.update(_referenced_matrix_axes(value))
    elif isinstance(node, list):
        for value in node:
            axes.update(_referenced_matrix_axes(value))
    return axes


def _decode_fromjson_matrix(value: str) -> Any | None:
    """Decode a literal ``${{ fromJSON('...') }}`` matrix, else ``None``."""
    match = FROMJSON_MATRIX_RE.fullmatch(value.strip())
    if match is None:
        return None
    try:
        return json.loads(match.group("payload"))
    except (TypeError, ValueError):
        return None


def _runtime_pins(
    node: Any,
    location: str,
    *,
    in_matrix: bool = False,
    matrix_keys: set[str] | None = None,
) -> Iterator[tuple[str, Any]]:
    matrix_keys = MATRIX_RUNTIME_KEYS if matrix_keys is None else matrix_keys
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{location}.{key}"
            child_keys = matrix_keys
            if location.endswith(".jobs") and isinstance(value, dict):
                child_keys = MATRIX_RUNTIME_KEYS | _referenced_matrix_axes(value)
            entering_matrix = key == "matrix" and location.endswith(".strategy")
            if key in RUNTIME_KEYS or (in_matrix and key in matrix_keys):
                yield path, value
            matrix_expression = entering_matrix or (in_matrix and key == "include")
            if matrix_expression and isinstance(value, str):
                decoded = _decode_fromjson_matrix(value)
                if decoded is None:
                    # Reject matrices the scanner cannot inspect so they cannot hide 3.12.
                    yield path, OPAQUE_MATRIX
                    continue
                yield from _runtime_pins(decoded, path, in_matrix=True, matrix_keys=child_keys)
                continue
            yield from _runtime_pins(
                value,
                path,
                in_matrix=in_matrix or entering_matrix,
                matrix_keys=child_keys,
            )
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _runtime_pins(
                value, f"{location}[{index}]", in_matrix=in_matrix, matrix_keys=matrix_keys
            )


def _contains_old_runtime(value: Any) -> bool:
    if value is OPAQUE_MATRIX:
        return True
    if isinstance(value, list):
        return any(_contains_old_runtime(item) for item in value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            pass
        else:
            if decoded != value:
                return _contains_old_runtime(decoded)
    return str(value) == "3.12" or str(value).startswith("3.12.")


def test_workflow_python_pins_follow_standard() -> None:
    workflows = {
        path.name: yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in sorted(WORKFLOWS.iterdir())
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    }
    assert workflows, "No workflow YAML was inspected"
    offenders = [
        location
        for filename, workflow in workflows.items()
        for location, value in _runtime_pins(workflow, filename)
        if _contains_old_runtime(value)
    ]
    assert not offenders, f"Python 3.12 runtime pins remain: {offenders}"
    for filename, job in (("ci.yml", "python"), ("pr-00-gate.yml", "python-ci")):
        config = workflows[filename]["jobs"][job]
        assert json.loads(config["with"]["python-versions"]) == ["3.13", "3.14"]
        assert not config.get("continue-on-error", False)


@pytest.mark.parametrize(
    "matrix, step_ref",
    [
        ({"python": ["3.12"]}, "${{ matrix.python }}"),
        ({"python": ["3.12.9", "3.14"]}, "${{ matrix.python }}"),
        ({"include": [{"python": "3.12", "os": "ubuntu-latest"}]}, "${{ matrix.python }}"),
        ({"python_version": ["3.12"]}, "${{ matrix.python_version }}"),
        ({"python-version": ["3.12", "3.14"]}, "${{ matrix.python-version }}"),
    ],
)
def test_named_python_matrix_cannot_hide_old_runtime(matrix: dict[str, Any], step_ref: str) -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": matrix},
                "steps": [{"with": {"python-version": step_ref}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert ".strategy.matrix." in offenders[0]


@pytest.mark.parametrize(
    "matrix_expr",
    [
        '${{ fromJSON(\'{"python":["3.12"]}\') }}',
        '${{ fromJSON("{\\"python_version\\":[\\"3.12\\",\\"3.14\\"]}") }}',
        "${{ fromJSON(inputs.matrix) }}",
        "${{ inputs.python_matrix }}",
    ],
)
def test_expression_valued_matrix_cannot_hide_old_runtime(matrix_expr: str) -> None:
    """Literal fromJSON 3.12 pins and opaque matrix expressions must both fail the guard."""
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": matrix_expr},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert offenders[0].startswith("ci.yml.jobs.test.strategy.matrix")


def test_malformed_literal_fromjson_matrix_is_opaque() -> None:
    """A literal fromJSON payload that fails JSON decoding must be treated as opaque."""
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": "${{ fromJSON('{\"python\":[}') }}"},
            }
        }
    }
    values = [
        value
        for path, value in _runtime_pins(workflow, "ci.yml")
        if path == "ci.yml.jobs.test.strategy.matrix"
    ]
    assert values == [OPAQUE_MATRIX]
    assert _contains_old_runtime(OPAQUE_MATRIX)


@pytest.mark.parametrize("filename", ["ci.yml", "pr-00-gate.yml"])
def test_owned_caller_workflow_text_has_no_312_lines(filename: str) -> None:
    """Line-level guard matching the Nov 7 persistence acceptance read for owned callers."""
    text = (WORKFLOWS / filename).read_text(encoding="utf-8")
    offenders = [
        f"{filename}:{index}:{line.rstrip()}"
        for index, line in enumerate(text.splitlines(), start=1)
        if "3.12" in line
    ]
    assert not offenders, f"Lines matching 3.12 remain: {offenders}"


@pytest.mark.parametrize("filename, job", [("ci.yml", "python"), ("pr-00-gate.yml", "python-ci")])
def test_json_matrix_deliberate_break_and_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filename: str, job: str
) -> None:
    """The named guard must reject a restored 3.12 JSON pin in either caller."""
    for name in ("ci.yml", "pr-00-gate.yml"):
        (tmp_path / name).write_bytes((WORKFLOWS / name).read_bytes())
    monkeypatch.setattr(f"{__name__}.WORKFLOWS", tmp_path)
    test_workflow_python_pins_follow_standard()

    path = tmp_path / filename
    original = path.read_bytes()
    workflow = yaml.safe_load(original)
    workflow["jobs"][job]["with"]["python-versions"] = '["3.12", "3.14"]'
    path.write_text(yaml.safe_dump(workflow), encoding="utf-8")
    with pytest.raises(AssertionError, match="Python 3.12 runtime pins remain") as failure:
        test_workflow_python_pins_follow_standard()
    assert str(failure.value).splitlines()[0] == (
        f"Python 3.12 runtime pins remain: ['{filename}.jobs.{job}.with.python-versions']"
    )

    path.write_bytes(original)
    test_workflow_python_pins_follow_standard()


def test_unrelated_python_metadata_is_not_a_runtime_pin() -> None:
    assert list(_runtime_pins({"metadata": {"python": "3.12"}}, "ci.yml")) == []


def test_literal_fromjson_matrix_without_old_runtime_is_allowed() -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": '${{ fromJSON(\'{"python":["3.13","3.14"]}\') }}'},
                "steps": [{"with": {"python-version": "${{ matrix.python }}"}}],
            }
        }
    }
    offenders = [
        path for path, value in _runtime_pins(workflow, "ci.yml") if _contains_old_runtime(value)
    ]
    assert offenders == []


@pytest.mark.parametrize("reference", ["${{ matrix.py }}", "${{ matrix['py'] }}"])
def test_custom_runtime_matrix_axis_cannot_hide_old_runtime(reference: str) -> None:
    workflow = {
        "jobs": {
            "test": {
                "strategy": {"matrix": {"py": ["3.12"]}},
                "steps": [{"with": {"python-version": reference}}],
            }
        }
    }
    offenders = [
        path
        for path, value in _runtime_pins(workflow, "standalone.yml")
        if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert offenders[0].endswith(".strategy.matrix.py")


def test_custom_axis_without_python_runtime_reference_is_ignored() -> None:
    workflow = {"jobs": {"test": {"strategy": {"matrix": {"py": ["3.12"]}}}}}
    assert not any(
        _contains_old_runtime(value) for _, value in _runtime_pins(workflow, "standalone.yml")
    )


@pytest.mark.parametrize(
    "include",
    [
        "${{ fromJSON('[{\"python\":\"3.12\"}]') }}",
        "${{ fromJSON(inputs.include) }}",
        "${{ fromJSON('[{') }}",
    ],
)
def test_include_expression_cannot_hide_old_runtime(include: str) -> None:
    workflow = {"jobs": {"test": {"strategy": {"matrix": {"include": include}}}}}
    offenders = [
        path
        for path, value in _runtime_pins(workflow, "standalone.yml")
        if _contains_old_runtime(value)
    ]
    assert len(offenders) == 1
    assert ".strategy.matrix.include" in offenders[0]


def test_literal_include_expression_accepts_current_runtime() -> None:
    include = "${{ fromJSON('[{\"python\":\"3.14\"}]') }}"
    workflow = {"jobs": {"test": {"strategy": {"matrix": {"include": include}}}}}
    assert not any(
        _contains_old_runtime(value) for _, value in _runtime_pins(workflow, "standalone.yml")
    )
