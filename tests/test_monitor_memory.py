import csv
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "monitor_memory.py"


def _load_monitor_memory():
    spec = importlib.util.spec_from_file_location("monitor_memory", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load monitor_memory module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


monitor_memory = _load_monitor_memory()


def test_parse_proc_status_reads_rss_and_vmsize() -> None:
    payload = "\n".join(
        [
            "Name:\tpython",
            "VmSize:\t  123456 kB",
            "VmRSS:\t   65432 kB",
        ]
    )

    result = monitor_memory.parse_proc_status(payload)

    assert result["rss_kb"] == 65432
    assert result["vms_kb"] == 123456


def test_parse_proc_status_requires_fields() -> None:
    payload = "Name:\tpython\nVmRSS:\t   65432 kB\n"

    with pytest.raises(ValueError, match="VmRSS or VmSize"):
        monitor_memory.parse_proc_status(payload)


def test_write_samples_respects_max_samples(tmp_path, monkeypatch) -> None:
    samples = [(111, 222), (333, 444)]

    def fake_sample_memory(pid: int) -> tuple[int, int]:
        # Deterministic samples keep the CSV content predictable.
        return samples.pop(0)

    monkeypatch.setattr(monitor_memory, "sample_memory", fake_sample_memory)

    output_path = tmp_path / "memory.csv"
    monitor_memory.write_samples(
        pid=123,
        interval_s=0,
        duration_s=60,
        output_path=str(output_path),
        max_samples=2,
    )

    with output_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == ["timestamp", "rss_kb", "vms_kb", "pid"]
    assert rows[1][1:] == ["111", "222", "123"]
    assert rows[2][1:] == ["333", "444", "123"]
    assert len(rows) == 3
