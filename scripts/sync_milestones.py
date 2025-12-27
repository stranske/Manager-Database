import argparse
import json
import re
import subprocess
from pathlib import Path

MILESTONE_RE = re.compile(
    r"^\|\s*\*\*(M\d+)\*\*\s*\|\s*(.*?)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|"
)


def parse_milestones(path: str) -> list[tuple[str, str, str]]:
    """Return (id, title, due_date) tuples parsed from a markdown table."""
    milestones: list[tuple[str, str, str]] = []
    for line in Path(path).read_text().splitlines():
        m = MILESTONE_RE.match(line)
        if m:
            mid, title, due = m.groups()
            milestones.append((mid, title, due))
    return milestones


def ensure_milestone(mid: str, title: str, due_date: str, repo: str) -> int:
    """Create or update a milestone via ``gh`` CLI and return its number."""
    out = subprocess.run(
        ["gh", "api", f"repos/{repo}/milestones", "?state=all"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    existing = json.loads(out)
    for ms in existing:
        if ms["title"] == title:
            if not ms.get("due_on", "").startswith(due_date):
                subprocess.run(
                    [
                        "gh",
                        "api",
                        f"repos/{repo}/milestones/{ms['number']}",
                        "-X",
                        "PATCH",
                        "-f",
                        f"due_on={due_date}T00:00:00Z",
                    ],
                    check=True,
                )
            return ms["number"]
    resp = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/milestones",
            "-X",
            "POST",
            "-f",
            f"title={title}",
            "-f",
            f"due_on={due_date}T00:00:00Z",
        ],
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return json.loads(resp)["number"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync GitHub milestones")
    parser.add_argument("file", help="Path to Manager-Intel-Platform.md")
    parser.add_argument("--repo", required=True, help="owner/repo for API calls")
    args = parser.parse_args()

    for mid, title, due in parse_milestones(args.file):
        ensure_milestone(mid, title, due, args.repo)
