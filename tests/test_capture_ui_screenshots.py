from __future__ import annotations

from pathlib import Path

import pytest

from scripts.capture_ui_screenshots import (
    SCREENSHOT_TARGETS,
    assert_non_empty_pngs,
    capture_targets,
    wait_for_ui,
)


class FakePage:
    def __init__(self) -> None:
        self.urls: list[str] = []
        self.selectors: list[str] = []
        self.paths: list[Path] = []

    def goto(self, url: str, *, wait_until: str, timeout: int) -> None:
        assert wait_until == "networkidle"
        assert timeout == 123
        self.urls.append(url)

    def screenshot(self, *, path: str, full_page: bool) -> None:
        assert full_page is True
        screenshot_path = Path(path)
        screenshot_path.write_bytes(b"png")
        self.paths.append(screenshot_path)

    def wait_for_selector(self, selector: str, *, timeout: int) -> None:
        assert timeout == 123
        self.selectors.append(selector)


def test_fixed_capture_targets_exclude_research() -> None:
    assert [target.title for target in SCREENSHOT_TARGETS] == [
        "Dashboard",
        "Daily Report",
        "Search",
        "Upload",
    ]
    assert "research" not in {target.route.strip("/") for target in SCREENSHOT_TARGETS}


def test_capture_targets_visits_routes_and_requires_non_empty_pngs(tmp_path: Path) -> None:
    page = FakePage()

    capture_targets(page, base_url="http://ui.local/base", output_dir=tmp_path, timeout_ms=123)

    assert page.urls == [
        "http://ui.local/base/",
        "http://ui.local/base/daily-report",
        "http://ui.local/base/search",
        "http://ui.local/base/upload",
    ]
    assert page.selectors == [
        "text=Dashboard",
        "text=Daily Report",
        "text=Search",
        "text=Upload",
    ]
    assert sorted(path.name for path in page.paths) == [
        "daily-report.png",
        "dashboard.png",
        "search.png",
        "upload.png",
    ]


def test_assert_non_empty_pngs_fails_for_missing_or_empty_files(tmp_path: Path) -> None:
    (tmp_path / "dashboard.png").write_bytes(b"png")
    (tmp_path / "daily-report.png").write_bytes(b"")
    (tmp_path / "search.png").write_bytes(b"png")

    with pytest.raises(RuntimeError, match="Missing or empty UI screenshot"):
        assert_non_empty_pngs(tmp_path)


def test_wait_for_ui_retries_until_http_reachable(monkeypatch) -> None:
    calls = {"count": 0}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    def fake_urlopen(url: str, timeout: float) -> FakeResponse:
        calls["count"] += 1
        assert url == "http://ui.local"
        assert timeout == 0.1
        if calls["count"] == 1:
            raise OSError("warming")
        return FakeResponse()

    monotonic_values = iter([0.0, 0.0, 0.05, 0.05])
    monkeypatch.setattr("scripts.capture_ui_screenshots.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "scripts.capture_ui_screenshots.time.monotonic", lambda: next(monotonic_values)
    )
    monkeypatch.setattr("scripts.capture_ui_screenshots.time.sleep", lambda seconds: None)

    wait_for_ui("http://ui.local", timeout_s=1.0, interval_s=0.1)

    assert calls["count"] == 2
