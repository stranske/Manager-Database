"""Collect memory usage samples over a 24-hour window."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import os
from pathlib import Path

PROC_ROOT = "/proc"
_MONITOR_MEMORY_PATH = Path(__file__).resolve().parent / "monitor_memory.py"


def load_monitor_memory():
    """Load the monitor_memory module from the scripts directory."""
    spec = importlib.util.spec_from_file_location("monitor_memory", _MONITOR_MEMORY_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load monitor_memory module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


monitor_memory = load_monitor_memory()


def list_process_cmdlines() -> dict[int, str]:
    """Return a mapping of PID to command line for visible processes."""
    cmdlines: dict[int, str] = {}
    for entry in os.listdir(PROC_ROOT):
        if not entry.isdigit():
            continue
        pid = int(entry)
        cmdline_path = os.path.join(PROC_ROOT, entry, "cmdline")
        try:
            with open(cmdline_path, "rb") as handle:
                raw = handle.read()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue

        if not raw:
            continue

        cmdline = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        if cmdline:
            cmdlines[pid] = cmdline
    return cmdlines


def find_pid_by_substring(process_name: str, cmdlines: dict[int, str]) -> int:
    """Find the lowest PID whose command line contains the substring."""
    matches = [pid for pid, cmdline in cmdlines.items() if process_name in cmdline]
    if not matches:
        raise ValueError(f"No process command line matched '{process_name}'.")
    return min(matches)


def resolve_pid(pid: int | None, process_name: str | None) -> int:
    """Resolve a PID from explicit input or a process name substring."""
    if pid is not None:
        return pid
    if process_name:
        cmdlines = list_process_cmdlines()
        return find_pid_by_substring(process_name, cmdlines)
    raise ValueError("Provide --pid or --process-name to select a target process.")


def default_output_path(now: dt.datetime, output_dir: str) -> str:
    """Build a timestamped output path for memory samples."""
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    filename = f"memory_usage_{timestamp}.csv"
    return os.path.join(output_dir, filename)


def resolve_output_path(
    *,
    output: str | None,
    output_dir: str,
    now: dt.datetime,
) -> str:
    """Resolve the final output path, allowing directories as inputs."""
    if output is None:
        return default_output_path(now, output_dir)

    expanded = os.path.expanduser(output)
    # Allow passing a directory so operators can reuse a single flag.
    if os.path.isdir(expanded) or expanded.endswith(os.sep):
        normalized_dir = expanded.rstrip(os.sep)
        return default_output_path(now, normalized_dir)
    return expanded


def run_collection(
    *,
    pid: int,
    interval_s: int,
    duration_hours: float,
    output_path: str,
    max_samples: int | None,
) -> None:
    """Collect memory usage for the requested interval."""
    duration_s = int(duration_hours * 60 * 60)
    monitor_memory.write_samples(
        pid=pid,
        interval_s=interval_s,
        duration_s=duration_s,
        output_path=output_path,
        max_samples=max_samples,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect memory usage samples over a 24-hour window."
    )
    parser.add_argument(
        "--pid",
        type=int,
        default=None,
        help="Process ID to monitor (default: resolve from --process-name).",
    )
    parser.add_argument(
        "--process-name",
        default=None,
        help="Substring of the process command line to monitor.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Sampling interval in seconds (default: 60).",
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=24.0,
        help="Total sampling duration in hours (default: 24).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Explicit output CSV path (default: timestamped file).",
    )
    parser.add_argument(
        "--output-dir",
        default="monitoring",
        help="Directory for timestamped outputs (default: monitoring).",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=None,
        help="Stop after this many samples (default: unlimited).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    pid = resolve_pid(args.pid, args.process_name)
    output_path = resolve_output_path(
        output=args.output,
        output_dir=args.output_dir,
        now=dt.datetime.utcnow(),
    )

    run_collection(
        pid=pid,
        interval_s=args.interval_seconds,
        duration_hours=args.duration_hours,
        output_path=output_path,
        max_samples=args.max_samples,
    )


if __name__ == "__main__":
    main()
