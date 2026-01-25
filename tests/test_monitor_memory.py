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
