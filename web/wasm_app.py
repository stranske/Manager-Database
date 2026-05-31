"""Offline stlite entrypoint for deterministic Manager-Database pages."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import streamlit as st

from ui import daily_report, dashboard, search, upload

OFFLINE_PAGE_TITLES = ["Dashboard", "Daily Report", "Search", "Upload"]


def _db_asset_path() -> str:
    candidates = [
        Path("manager_demo.sqlite"),
        Path(__file__).resolve().with_name("manager_demo.sqlite"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return "manager_demo.sqlite"


def _build_offline_pages() -> list[Any]:
    return [
        st.Page(dashboard.main, title=OFFLINE_PAGE_TITLES[0], icon="📈", url_path="", default=True),
        st.Page(
            daily_report.main,
            title=OFFLINE_PAGE_TITLES[1],
            icon="🗞️",
            url_path="daily-report",
        ),
        st.Page(search.main, title=OFFLINE_PAGE_TITLES[2], icon="🔎", url_path="search"),
        st.Page(upload.main, title=OFFLINE_PAGE_TITLES[3], icon="📝", url_path="upload"),
    ]


def main() -> None:
    os.environ.setdefault("DB_PATH", _db_asset_path())
    os.environ.setdefault("UI_OFFLINE", "1")
    os.environ.setdefault("USE_SIMPLE_EMBED", "1")
    os.environ.pop("DB_URL", None)
    navigation = st.navigation(_build_offline_pages(), position="sidebar")
    navigation.run()


if __name__ == "__main__":
    main()
