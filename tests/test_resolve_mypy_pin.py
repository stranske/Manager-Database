from __future__ import annotations

import builtins
from pathlib import Path

import pytest

from tools import resolve_mypy_pin


def test_get_mypy_python_version_reads_tool_mypy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("pyproject.toml").write_text(
        """
[tool.mypy]
python_version = "3.11"
""".lstrip(),
        encoding="utf-8",
    )

    assert resolve_mypy_pin.get_mypy_python_version() == "3.11"


def test_get_mypy_python_version_uses_regex_fallback_without_tomlkit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("pyproject.toml").write_text(
        """
[project]
name = "manager-database"

[tool.mypy]
ignore_missing_imports = true
python_version = '3.10'
""".lstrip(),
        encoding="utf-8",
    )
    real_import = builtins.__import__

    def import_without_tomlkit(name: str, *args: object, **kwargs: object) -> object:
        if name == "tomlkit":
            raise ImportError("tomlkit intentionally unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_tomlkit)

    assert resolve_mypy_pin.get_mypy_python_version() == "3.10"


def test_get_mypy_python_version_returns_none_without_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("pyproject.toml").write_text(
        """
[project]
name = "manager-database"
""".lstrip(),
        encoding="utf-8",
    )

    assert resolve_mypy_pin.get_mypy_python_version() is None


def test_main_uses_matrix_fallback_when_no_pyproject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MATRIX_PYTHON_VERSION", "3.13")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)

    assert resolve_mypy_pin.main() == 0

    assert capsys.readouterr().out == "python-version=3.13\n"


def test_main_writes_github_output_from_mypy_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    output_path = tmp_path / "github-output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    Path("pyproject.toml").write_text(
        """
[tool.mypy]
python_version = "3.12"
""".lstrip(),
        encoding="utf-8",
    )

    assert resolve_mypy_pin.main() == 0

    assert output_path.read_text(encoding="utf-8") == "python-version=3.12\n"
    assert capsys.readouterr().out == "Resolved mypy Python version: 3.12\n"


def test_main_defaults_to_primary_python_without_matrix_or_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MATRIX_PYTHON_VERSION", raising=False)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)

    assert resolve_mypy_pin.main() == 0

    assert capsys.readouterr().out == "python-version=3.12\n"
