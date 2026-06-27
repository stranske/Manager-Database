"""Tests for tools/resolve_mypy_pin.py."""

from __future__ import annotations

from pathlib import Path

import pytest

import tools.resolve_mypy_pin as resolve_mypy_pin


def _write_pyproject(tmp_path: Path, content: str) -> None:
    (tmp_path / "pyproject.toml").write_text(content, encoding="utf-8")


def test_get_mypy_python_version_reads_tool_mypy_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pyproject(
        tmp_path,
        '[tool.mypy]\npython_version = "3.11"\n',
    )
    monkeypatch.chdir(tmp_path)

    assert resolve_mypy_pin.get_mypy_python_version() == "3.11"


def test_get_mypy_python_version_returns_none_when_pyproject_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    assert resolve_mypy_pin.get_mypy_python_version() is None


def test_get_mypy_python_version_returns_none_without_mypy_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pyproject(tmp_path, '[project]\nname = "example"\n')
    monkeypatch.chdir(tmp_path)

    assert resolve_mypy_pin.get_mypy_python_version() is None


def test_get_mypy_python_version_regex_fallback_when_tomlkit_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pyproject(
        tmp_path,
        '[tool.mypy]\npython_version = "3.10"\n',
    )
    monkeypatch.chdir(tmp_path)

    real_import = __import__

    def _import(name, *args, **kwargs):
        if name == "tomlkit":
            raise ImportError("tomlkit unavailable for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _import)

    assert resolve_mypy_pin.get_mypy_python_version() == "3.10"


def test_main_uses_mypy_version_from_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_pyproject(tmp_path, '[tool.mypy]\npython_version = "3.11"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setenv("MATRIX_PYTHON_VERSION", "3.13")

    assert resolve_mypy_pin.main() == 0
    assert capsys.readouterr().out.strip() == "python-version=3.11"


def test_main_falls_back_to_matrix_python_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setenv("MATRIX_PYTHON_VERSION", "3.13")

    assert resolve_mypy_pin.main() == 0
    assert capsys.readouterr().out.strip() == "python-version=3.13"


def test_main_defaults_to_312_without_config_or_matrix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.delenv("MATRIX_PYTHON_VERSION", raising=False)

    assert resolve_mypy_pin.main() == 0
    assert capsys.readouterr().out.strip() == "python-version=3.12"


def test_main_writes_github_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_pyproject(tmp_path, '[tool.mypy]\npython_version = "3.11"\n')
    output_path = tmp_path / "github_output.txt"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setenv("MATRIX_PYTHON_VERSION", "3.13")

    assert resolve_mypy_pin.main() == 0
    assert output_path.read_text(encoding="utf-8") == "python-version=3.11\n"
    assert capsys.readouterr().out.strip() == "Resolved mypy Python version: 3.11"
