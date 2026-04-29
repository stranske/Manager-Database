"""Local readiness smoke for the Manager-Intel stack.

Hits the FastAPI surface that the docker-compose stack exposes (default
http://localhost:8000) and verifies that the database, object storage,
manager API, and chat/research path are all reachable. Designed to run
without external provider credentials so it can be invoked as the single
post-`docker compose up` validation step.

Exit codes:
    0 — all probes succeeded
    1 — at least one probe failed (message printed to stderr)
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

import httpx

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_TIMEOUT_S = 10.0


class ReadinessError(RuntimeError):
    """Raised when a readiness probe fails."""


def check_health(client: httpx.Client) -> dict[str, Any]:
    """Verify /health/detailed reports app/database/minio healthy."""
    resp = client.get("/health/detailed")
    if resp.status_code != 200:
        raise ReadinessError(
            f"/health/detailed returned {resp.status_code}: {resp.text[:300]}"
        )
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
    """Verify the manager API returns at least one seeded record."""
    resp = client.get("/managers", params={"limit": 1})
    if resp.status_code != 200:
        raise ReadinessError(f"/managers returned {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    items = body.get("items") if isinstance(body, dict) else None
    if not items:
        raise ReadinessError(
            "/managers returned no records — run `python scripts/seed_managers.py` "
            "to seed the baseline managers before invoking the smoke."
        )
    return body


def check_chat(client: httpx.Client) -> dict[str, Any]:
    """Verify the chat/research endpoint answers a deterministic local query."""
    resp = client.get("/chat", params={"q": "readiness smoke"})
    if resp.status_code != 200:
        raise ReadinessError(f"/chat returned {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    if "answer" not in body:
        raise ReadinessError(f"/chat response missing 'answer' field: {body}")
    return body


def run(base_url: str, timeout_s: float) -> int:
    with httpx.Client(base_url=base_url, timeout=timeout_s) as client:
        check_health(client)
        check_managers(client)
        check_chat(client)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=DEFAULT_API_BASE,
        help=f"FastAPI base URL (default: {DEFAULT_API_BASE})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_S})",
    )
    args = parser.parse_args(argv)
    try:
        run(args.base_url, args.timeout)
    except ReadinessError as exc:
        print(f"readiness smoke FAILED: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPError as exc:
        print(f"readiness smoke FAILED (transport): {exc}", file=sys.stderr)
        return 1
    print(f"readiness smoke OK ({args.base_url})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
