"""Regression tests for the dependency setup validator."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def test_validator_uses_current_dependabot_lock_workflow() -> None:
    validator = Path("scripts/validate_dependency_test_setup.py").read_text(encoding="utf-8")

    assert ".github/workflows/maint-dependabot-auto-lock.yml" in validator
    assert ".github/workflows/dependabot-auto-lock.yml" not in validator


def test_validator_no_longer_checks_stale_trend_or_streamlit_paths() -> None:
    validator = Path("scripts/validate_dependency_test_setup.py").read_text(encoding="utf-8")

    assert "src/trend_analysis" not in validator
    assert "streamlit_app" not in validator


def test_check_test_dependencies_recommends_existing_commands() -> None:
    helper = Path("scripts/check_test_dependencies.sh").read_text(encoding="utf-8")

    assert "./scripts/run_tests.sh" not in helper

    recommended_script_paths = {
        line.split("./", 1)[1].strip() for line in helper.splitlines() if "./scripts/" in line
    }

    missing = sorted(path for path in recommended_script_paths if not Path(path).is_file())
    assert not missing


def test_validator_passes_on_current_repository() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/validate_dependency_test_setup.py"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    assert result.returncode == 0, result.stdout
    assert "maint-dependabot-auto-lock.yml includes all non-empty extras" in result.stdout


def test_validator_fails_when_current_dependabot_workflow_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path
    workflow_dir = repo / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        """
[project]
name = "fixture"
version = "0.1.0"

[project.optional-dependencies]
dev = ["pytest"]
""".strip(),
        encoding="utf-8",
    )
    (workflow_dir / "dependabot-auto-lock.yml").write_text(
        "run: uv pip compile pyproject.toml --extra dev\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(repo)
    import scripts.validate_dependency_test_setup as validator

    passed, issues = validator.check_lock_file_completeness()

    assert not passed
    assert "maint-dependabot-auto-lock.yml not found" in issues
