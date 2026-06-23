from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


class FakeStreamlit:
    def __init__(self) -> None:
        self.session_state: dict[str, bool] = {}
        self.warning_messages: list[str] = []

    def warning(self, message: str) -> None:
        self.warning_messages.append(message)


def _load_ui_module():
    return importlib.reload(importlib.import_module("ui"))


def test_missing_ui_credentials_do_not_render_main_warning(monkeypatch, caplog) -> None:
    ui = _load_ui_module()
    fake_st = FakeStreamlit()
    monkeypatch.delenv("UI_USERNAME", raising=False)
    monkeypatch.delenv("UI_PASSWORD", raising=False)
    monkeypatch.setattr(ui, "st", fake_st)
    caplog.set_level(logging.INFO, logger="ui")

    assert ui.require_login() is True
    assert fake_st.session_state["auth"] is True
    assert fake_st.warning_messages == []
    assert "UI_USERNAME/UI_PASSWORD not set; skipping authentication in dev mode." in caplog.text
