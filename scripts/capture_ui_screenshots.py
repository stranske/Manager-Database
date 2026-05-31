"""Capture deterministic UI screenshots for CI artifacts."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urljoin
from urllib.request import urlopen


@dataclass(frozen=True)
class ScreenshotTarget:
    title: str
    route: str
    filename: str


SCREENSHOT_TARGETS = [
    ScreenshotTarget("Dashboard", "/", "dashboard.png"),
    ScreenshotTarget("Daily Report", "/daily-report", "daily-report.png"),
    ScreenshotTarget("Search", "/search", "search.png"),
    ScreenshotTarget("Upload", "/upload", "upload.png"),
]


class Page(Protocol):
    def goto(self, url: str, *, wait_until: str, timeout: int) -> object: ...

    def screenshot(self, *, path: str, full_page: bool) -> object: ...


def expected_png_paths(output_dir: Path) -> list[Path]:
    """Return the required screenshot files."""
    return [output_dir / target.filename for target in SCREENSHOT_TARGETS]


def assert_non_empty_pngs(output_dir: Path) -> None:
    """Fail when any expected screenshot is missing or empty."""
    missing_or_empty = [
        path for path in expected_png_paths(output_dir) if not path.exists() or path.stat().st_size == 0
    ]
    if missing_or_empty:
        formatted = ", ".join(str(path) for path in missing_or_empty)
        raise RuntimeError(f"Missing or empty UI screenshot artifact(s): {formatted}")


def wait_for_ui(base_url: str, *, timeout_s: float = 60.0, interval_s: float = 1.0) -> None:
    """Wait for the Streamlit server to accept HTTP requests."""
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urlopen(base_url, timeout=interval_s) as response:
                if 200 <= response.status < 500:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(interval_s)
    raise RuntimeError(f"UI did not become reachable at {base_url}: {last_error}")


def capture_targets(page: Page, *, base_url: str, output_dir: Path, timeout_ms: int) -> None:
    """Capture the fixed deterministic page set, excluding Research."""
    output_dir.mkdir(parents=True, exist_ok=True)
    base = base_url.rstrip("/") + "/"
    for target in SCREENSHOT_TARGETS:
        url = urljoin(base, target.route.lstrip("/"))
        page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        page.screenshot(path=str(output_dir / target.filename), full_page=True)
    assert_non_empty_pngs(output_dir)


def capture_with_playwright(*, base_url: str, output_dir: Path, timeout_ms: int) -> None:
    """Launch Chromium through Playwright and write screenshots."""
    from playwright.sync_api import sync_playwright

    wait_for_ui(base_url, timeout_s=timeout_ms / 1000)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1100})
            capture_targets(page, base_url=base_url, output_dir=output_dir, timeout_ms=timeout_ms)
        finally:
            browser.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8501")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/ui"))
    parser.add_argument("--timeout-ms", type=int, default=60_000)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        capture_with_playwright(
            base_url=args.base_url,
            output_dir=args.output_dir,
            timeout_ms=args.timeout_ms,
        )
    except Exception as exc:
        print(f"UI screenshot capture failed: {exc}", file=sys.stderr)
        return 1
    print(f"Captured UI screenshots in {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
