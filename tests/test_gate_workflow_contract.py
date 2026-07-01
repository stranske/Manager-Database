from pathlib import Path
from typing import Any, cast

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _workflow(path: str) -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8")))


def _numeric(value: object) -> float:
    if not isinstance(value, (str, bytes, bytearray, int, float)):
        raise AssertionError(f"Expected numeric workflow value, got {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"Expected numeric workflow value, got {value!r}") from exc


def test_pr_gate_python_ci_matches_main_ci_contract() -> None:
    pr_gate = _workflow(".github/workflows/pr-00-gate.yml")
    main_ci = _workflow(".github/workflows/ci.yml")

    pr_python = pr_gate["jobs"]["python-ci"]["with"]
    main_python = main_ci["jobs"]["python"]["with"]

    assert pr_python["typecheck"] is True
    assert pr_python["typecheck"] == main_python["typecheck"]
    assert pr_python["coverage"] is True
    assert pr_python["coverage"] == main_python["coverage"]
    assert _numeric(pr_python["coverage-min"]) >= _numeric(main_python["coverage-min"])

    if "format_check" in pr_python:
        assert pr_python["format_check"] is not False


def test_gate_summary_depends_on_python_ci() -> None:
    pr_gate = _workflow(".github/workflows/pr-00-gate.yml")

    summary_needs = pr_gate["jobs"]["summary"]["needs"]

    assert "detect" in summary_needs
    assert "python-ci" in summary_needs
