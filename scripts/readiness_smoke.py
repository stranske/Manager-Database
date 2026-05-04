"""Local readiness smoke for the Manager-Intel stack.

Hits the FastAPI surface that the docker-compose stack exposes (default
http://localhost:8000), the Streamlit UI (default http://localhost:8501),
and verifies that the database, object storage, manager API, and
chat/research path are all reachable. Designed to run without external
provider credentials so it can be invoked as the single clean-stack
validation step.

Exit codes:
    0 — all probes succeeded
    1 — at least one probe failed (message printed to stderr)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from typing import Any

import httpx

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_UI_BASE = "http://localhost:8501"
DEFAULT_TIMEOUT_S = 10.0
DEFAULT_COMPOSE_SERVICES = ("db", "minio", "api", "ui")
DEFAULT_CHAT_QUERY = "readiness smoke deterministic fact"
EXPECTED_CHAT_SNIPPET = "Readiness smoke deterministic fact"
EXPECTED_MANAGER_NAME = "Elliott Investment Management L.P."


class ReadinessError(RuntimeError):
    """Raised when a readiness probe fails."""


def _run_cmd(cmd: list[str], cwd: str | None = None, env: dict[str, str] | None = None) -> None:
    try:
        subprocess.run(cmd, check=True, cwd=cwd, env=env)
    except subprocess.CalledProcessError as exc:
        joined = " ".join(cmd)
        raise ReadinessError(f"command failed ({exc.returncode}): {joined}") from exc


def bring_up_clean_stack(compose_file: str = "docker-compose.yml") -> None:
    """Reset compose state and start the local readiness services."""
    _run_cmd(["docker", "compose", "-f", compose_file, "down", "-v"])
    _run_cmd(
        [
            "docker",
            "compose",
            "-f",
            compose_file,
            "up",
            "-d",
            *DEFAULT_COMPOSE_SERVICES,
        ]
    )


def seed_local_readiness_data(
    compose_file: str = "docker-compose.yml", *, in_compose: bool = False
) -> None:
    """Seed deterministic local records used by readiness probes."""
    env = os.environ.copy()
    env.setdefault("USE_SIMPLE_EMBED", "1")
    if in_compose:
        _run_cmd(
            [
                "docker",
                "compose",
                "-f",
                compose_file,
                "exec",
                "-T",
                "-e",
                "USE_SIMPLE_EMBED=1",
                "api",
                "python",
                "scripts/seed_readiness_data.py",
            ]
        )
        return
    _run_cmd(["python", "scripts/seed_readiness_data.py"], cwd=".", env=env)


def check_health(client: httpx.Client) -> dict[str, Any]:
    """Verify /health/detailed reports app/database/minio healthy."""
    resp = client.get("/health/detailed")
    if resp.status_code != 200:
        raise ReadinessError(f"/health/detailed returned {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    if not body.get("healthy"):
        raise ReadinessError(f"/health/detailed reports unhealthy: {body}")
    components = body.get("components") or {}
    # The detailed health endpoint groups dependencies under "app",
    # "database", and "minio". Redis is optional and only enforced when
    # REDIS_URL is set.
    for name in ("app", "database", "minio"):
        comp = components.get(name) or {}
        if not comp.get("healthy"):
            raise ReadinessError(f"component {name!r} unhealthy: {comp}")
    return body


def check_managers(client: httpx.Client) -> dict[str, Any]:
    """Verify the manager API returns deterministic seeded records."""
    resp = client.get("/managers", params={"limit": 100, "offset": 0})
    if resp.status_code != 200:
        raise ReadinessError(f"/managers returned {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    items = body.get("items") if isinstance(body, dict) else None
    if not items:
        raise ReadinessError(
            "/managers returned no records — run `python scripts/seed_managers.py` "
            "to seed the baseline managers before invoking the smoke."
        )
    manager_names = {
        str(item.get("name", ""))
        for item in items
        if isinstance(item, dict)
    }
    if EXPECTED_MANAGER_NAME not in manager_names:
        raise ReadinessError(
            f"/managers response missing expected seeded manager {EXPECTED_MANAGER_NAME!r}"
        )
    return body


def check_chat(client: httpx.Client) -> dict[str, Any]:
    """Verify the chat/research endpoint answers a deterministic local query."""
    resp = client.get("/chat", params={"q": DEFAULT_CHAT_QUERY})
    if resp.status_code != 200:
        raise ReadinessError(f"/chat returned {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    if "answer" not in body:
        raise ReadinessError(f"/chat response missing 'answer' field: {body}")
    answer = str(body.get("answer", ""))
    if EXPECTED_CHAT_SNIPPET not in answer:
        raise ReadinessError(
            f"/chat answer missing deterministic seeded snippet {EXPECTED_CHAT_SNIPPET!r}: {answer[:300]}"
        )
    return body


def check_ui(base_url: str, timeout_s: float) -> None:
    """Verify the Streamlit UI service is reachable."""
    resp = httpx.get(base_url, timeout=timeout_s)
    if resp.status_code >= 400:
        raise ReadinessError(f"UI returned {resp.status_code}: {resp.text[:300]}")


def run(
    base_url: str,
    ui_url: str,
    timeout_s: float,
    clean_stack: bool,
    compose_file: str,
) -> int:
    if clean_stack:
        bring_up_clean_stack(compose_file)
    seed_local_readiness_data(compose_file, in_compose=clean_stack)
    with httpx.Client(base_url=base_url, timeout=timeout_s) as client:
        check_health(client)
        check_managers(client)
        check_chat(client)
    check_ui(ui_url, timeout_s)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=DEFAULT_API_BASE,
        help=f"FastAPI base URL (default: {DEFAULT_API_BASE})",
    )
    parser.add_argument(
        "--ui-url",
        default=DEFAULT_UI_BASE,
        help=f"Streamlit UI base URL (default: {DEFAULT_UI_BASE})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_S})",
    )
    parser.add_argument(
        "--compose-file",
        default="docker-compose.yml",
        help="Compose file used for clean-stack startup and in-container seeding.",
    )
    parser.add_argument(
        "--skip-stack-start",
        action="store_true",
        help="Probe an already-running stack and seed through the local Python environment.",
    )
    args = parser.parse_args(argv)
    try:
        run(
            args.base_url,
            args.ui_url,
            args.timeout,
            clean_stack=not args.skip_stack_start,
            compose_file=args.compose_file,
        )
    except ReadinessError as exc:
        print(f"readiness smoke FAILED: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPError as exc:
        print(f"readiness smoke FAILED (transport): {exc}", file=sys.stderr)
        return 1
    print(f"readiness smoke OK (api={args.base_url}, ui={args.ui_url})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
