import datetime as dt
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "collect_memory_24h.py"


def _load_collect_memory():
    spec = importlib.util.spec_from_file_location("collect_memory_24h", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load collect_memory_24h module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collect_memory = _load_collect_memory()


def test_find_pid_by_substring_selects_lowest() -> None:
    cmdlines = {200: "python app.py", 50: "python api.chat", 75: "bash"}

    result = collect_memory.find_pid_by_substring("python", cmdlines)

    assert result == 50


def test_find_pid_by_substring_requires_match() -> None:
    with pytest.raises(ValueError, match="No process command line matched"):
        collect_memory.find_pid_by_substring("uvicorn", {10: "python app.py"})


def test_resolve_pid_prefers_explicit(monkeypatch) -> None:
    def fail_list():
        raise AssertionError("list_process_cmdlines should not be called")

    monkeypatch.setattr(collect_memory, "list_process_cmdlines", fail_list)

    assert collect_memory.resolve_pid(1234, "python") == 1234


def test_resolve_pid_from_process_name(monkeypatch) -> None:
    monkeypatch.setattr(collect_memory, "list_process_cmdlines", lambda: {42: "uvicorn api.chat"})

    assert collect_memory.resolve_pid(None, "api.chat") == 42


def test_default_output_path_uses_timestamp() -> None:
    now = dt.datetime(2024, 1, 2, 3, 4, 5)
    output = collect_memory.default_output_path(now, "monitoring")

    assert output == "monitoring/memory_usage_20240102T030405Z.csv"


def test_resolve_output_path_prefers_explicit_file(tmp_path) -> None:
    now = dt.datetime(2024, 1, 2, 3, 4, 5)
    explicit = tmp_path / "memory.csv"

    output = collect_memory.resolve_output_path(
        output=str(explicit),
        output_dir=str(tmp_path),
        now=now,
    )

    assert output == str(explicit)


def test_resolve_output_path_accepts_directory(tmp_path) -> None:
    now = dt.datetime(2024, 1, 2, 3, 4, 5)

    output = collect_memory.resolve_output_path(
        output=str(tmp_path),
        output_dir="ignored",
        now=now,
    )

    # Directory inputs should be expanded into timestamped filenames.
    assert output == str(tmp_path / "memory_usage_20240102T030405Z.csv")


def test_resolve_output_path_accepts_trailing_separator(tmp_path) -> None:
    now = dt.datetime(2024, 1, 2, 3, 4, 5)
    output_dir = f"{tmp_path}{collect_memory.os.sep}"

    output = collect_memory.resolve_output_path(
        output=output_dir,
        output_dir="ignored",
        now=now,
    )

    assert output == str(tmp_path / "memory_usage_20240102T030405Z.csv")


def test_run_collection_calls_monitor(monkeypatch) -> None:
    captured = {}

    def fake_write_samples(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(collect_memory.monitor_memory, "write_samples", fake_write_samples)

    collect_memory.run_collection(
        pid=7,
        interval_s=15,
        duration_hours=0.5,
        output_path="monitoring/sample.csv",
        max_samples=10,
    )

    assert captured == {
        "pid": 7,
        "interval_s": 15,
        "duration_s": 1800,
        "output_path": "monitoring/sample.csv",
        "max_samples": 10,
    }
