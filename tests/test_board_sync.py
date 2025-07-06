import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.board_sync import update_status


def test_update_status(monkeypatch):
    called = {}

    def fake_run(cmd, check=True):
        called["cmd"] = cmd
        called["check"] = check

        class Proc:
            returncode = 0

        return Proc()

    monkeypatch.setattr(subprocess, "run", fake_run)
    update_status("123", "Done", project="Manager-Intel")
    assert called["cmd"] == [
        "gh",
        "project",
        "item-status",
        "--project",
        "Manager-Intel",
        "--item",
        "123",
        "--status",
        "Done",
    ]
    assert called["check"] is True
