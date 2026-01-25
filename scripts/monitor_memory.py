"""Collect memory usage samples for a running process.

Scope: Linux /proc-based RSS/VMS sampling for a single PID over a bounded window.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import time

PROC_STATUS_PATH = "/proc/{pid}/status"
DEFAULT_SAMPLE_INTERVAL_S = 60
DEFAULT_DURATION_S = 24 * 60 * 60
DEFAULT_OUTPUT_PATH = "monitoring/memory_usage.csv"
DEFAULT_COLUMNS = ("timestamp", "rss_kb", "vms_kb", "pid")


def parse_proc_status(text: str) -> dict[str, int]:
    """Parse VmRSS and VmSize values (in kB) from /proc/<pid>/status."""
    rss_kb = None
    vms_kb = None
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            # Expected format: "VmRSS:   12345 kB".
            rss_kb = int(line.split()[1])
        elif line.startswith("VmSize:"):
            # VmSize captures virtual memory size, complementary to RSS.
            vms_kb = int(line.split()[1])

    if rss_kb is None or vms_kb is None:
        raise ValueError("Missing VmRSS or VmSize in /proc status payload")

    return {"rss_kb": rss_kb, "vms_kb": vms_kb}


def read_proc_status(pid: int) -> str:
    """Read the /proc status file for a PID."""
    status_path = PROC_STATUS_PATH.format(pid=pid)
    with open(status_path, encoding="utf-8") as handle:
        return handle.read()


def sample_memory(pid: int) -> tuple[int, int]:
    """Return (rss_kb, vms_kb) for the given PID using /proc."""
    payload = read_proc_status(pid)
    parsed = parse_proc_status(payload)
    return parsed["rss_kb"], parsed["vms_kb"]


def ensure_parent_dir(path: str) -> None:
    """Create the parent directory for a file path if needed."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def write_samples(
    pid: int,
    interval_s: int,
    duration_s: int,
    output_path: str,
    *,
    max_samples: int | None = None,
) -> None:
    """Write timestamped memory samples to a CSV file."""
    ensure_parent_dir(output_path)

    file_exists = os.path.exists(output_path)
    end_time = time.time() + duration_s
    samples_written = 0

    with open(output_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            # Keep the header consistent for downstream analysis scripts.
            writer.writerow(DEFAULT_COLUMNS)

        while True:
            now = time.time()
            if now > end_time:
                break

            rss_kb, vms_kb = sample_memory(pid)
            timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
            # Persist each sample immediately to reduce data loss risk.
            writer.writerow([timestamp, rss_kb, vms_kb, pid])
            handle.flush()
            samples_written += 1
            # Allow bounded sampling for quick validations and tests.
            if max_samples is not None and samples_written >= max_samples:
                break
            time.sleep(interval_s)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record memory usage for a running process to CSV."
    )
    parser.add_argument(
        "--pid",
        type=int,
        default=os.getpid(),
        help="Process ID to monitor (default: current process).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=DEFAULT_SAMPLE_INTERVAL_S,
        help="Sampling interval in seconds (default: 60).",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=DEFAULT_DURATION_S,
        help="Total sampling duration in seconds (default: 86400).",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=None,
        help="Stop after this many samples (default: unlimited).",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH,
        help="CSV output path (default: monitoring/memory_usage.csv).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    write_samples(
        pid=args.pid,
        interval_s=args.interval_seconds,
        duration_s=args.duration_seconds,
        output_path=args.output,
        max_samples=args.max_samples,
    )


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
