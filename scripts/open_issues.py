import argparse
import re
import subprocess
from pathlib import Path

TASK_RE = re.compile(r"^\s*(?:\d+\.|\*)\s+(.*)")
# Match headings like "### 4.2 Stage 1 — Proof" and capture the Stage number.
STAGE_RE = re.compile(r"^###\s+4\.\d+\s+Stage\s+(\d+)")


def parse_tasks(path: str) -> list[tuple[str, str]]:
    """Return (stage, task) tuples parsed from Agents.md."""
    tasks: list[tuple[str, str]] = []
    stage = None
    for line in Path(path).read_text().splitlines():
        m = STAGE_RE.match(line)
        if m:
            stage = f"Stage{m.group(1)}"
            continue
        m = TASK_RE.match(line)
        if stage and m:
            task = m.group(1).strip()
            task = re.sub(r"`([^`]+)`", r"\1", task)
            tasks.append((stage, task))
    return tasks


def create_issue(stage: str, task: str) -> None:
    """Open a GitHub issue via ``gh`` CLI."""
    title = f"{stage}: {task}"
    subprocess.run(
        [
            "gh",
            "issue",
            "create",
            "--title",
            title,
            "--body",
            f"Auto-generated from Agents.md for {stage}",
        ],
        check=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Issues from Agents.md")
    parser.add_argument("file", help="Path to Agents.md")
    parser.add_argument("--dry-run", action="store_true", help="Print tasks only")
    args = parser.parse_args()
    for stage, task in parse_tasks(args.file):
        if args.dry_run:
            print(f"{stage}: {task}")
        else:
            create_issue(stage, task)
