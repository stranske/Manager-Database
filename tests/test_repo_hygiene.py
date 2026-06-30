from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_root_generated_clutter_is_not_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files", "node_modules", ".coverage", "codex-prompt-*.md"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout == ""
