"""Tests for the internal/local UI launcher (`make app` / `mgrdb-app`)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui import launch


def test_ui_launch_command_matches_compose_cmd():
    # Must match ui/Dockerfile CMD and the Makefile `app` target verbatim.
    assert launch.ui_launch_command() == [
        "streamlit",
        "run",
        "ui/app.py",
        "--server.port=8501",
        "--server.headless=true",
    ]


def test_ui_launch_command_honors_port():
    assert "--server.port=9000" in launch.ui_launch_command(port=9000)


def test_main_invokes_streamlit_via_subprocess(monkeypatch):
    seen: dict[str, list[str]] = {}

    def fake_call(cmd):
        seen["cmd"] = cmd
        return 0

    monkeypatch.setattr(launch.subprocess, "call", fake_call)
    assert launch.main() == 0
    assert seen["cmd"][0] == "streamlit"
    assert seen["cmd"][2] == "ui/app.py"
