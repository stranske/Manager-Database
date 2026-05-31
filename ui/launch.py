"""Console-script launcher for the Manager-Database analyst UI.

Runs the exact command the docker-compose ``ui`` service uses
(``ui/Dockerfile`` ``CMD``) so ``make app``, the ``mgrdb-app`` console
script, and the container all start the Streamlit multipage shell the same
way. This is the lightweight internal/local launch path: it does **not**
bring up Postgres/MinIO/uvicorn, so the UI renders against whatever API
targets ``API_BASE_URL`` and ``CHAT_API_URL`` point at. ``API_BASE_URL``
defaults to ``http://localhost:8000``; ``CHAT_API_URL`` is a direct endpoint
and defaults to ``http://localhost:8000/api/chat``. The UI cannot render live
data without a reachable API on ``:8000``.

Auth is bypassed automatically when ``UI_USERNAME``/``UI_PASSWORD`` are
unset (see ``ui/__init__.py``), which is the intended local/internal mode;
production keeps those credentials set.
"""

from __future__ import annotations

import subprocess
import sys

UI_APP_PATH = "ui/app.py"
DEFAULT_UI_PORT = 8501


def ui_launch_command(port: int = DEFAULT_UI_PORT) -> list[str]:
    """Return the Streamlit launch command, matching ``ui/Dockerfile`` ``CMD``."""
    return [
        "streamlit",
        "run",
        UI_APP_PATH,
        f"--server.port={port}",
        "--server.headless=true",
    ]


def main(argv: list[str] | None = None) -> int:
    """Launch the analyst UI shell on ``:8501`` for internal/local use."""
    return subprocess.call(ui_launch_command())


if __name__ == "__main__":
    sys.exit(main())
