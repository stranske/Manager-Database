from __future__ import annotations

from datetime import date

import pytest

from ui import alerts as alerts_ui


def test_load_managers_pages_and_sorts_results(monkeypatch):
    calls: list[tuple[str, str, dict]] = []

    def _fake_api_request(method: str, path: str, *, params=None, json_body=None):
        calls.append((method, path, params or {}))
        offset = int((params or {}).get("offset", 0))
        if offset == 0:
            return True, {
                "items": [{"id": 2, "name": "Zed"}, {"id": 1, "name": "Amy"}],
                "total": 3,
            }
        return True, {"items": [{"id": 3, "name": "Bob"}], "total": 3}

    monkeypatch.setattr(alerts_ui, "_api_request", _fake_api_request)
    alerts_ui._load_managers.clear()

    managers = alerts_ui._load_managers()

    assert managers == [(1, "Amy"), (3, "Bob"), (2, "Zed")]
    assert calls == [
        ("GET", "/managers", {"limit": 100, "offset": 0}),
        ("GET", "/managers", {"limit": 100, "offset": 2}),
    ]


def test_load_managers_returns_empty_on_api_error(monkeypatch):
    monkeypatch.setattr(
        alerts_ui, "_api_request", lambda *args, **kwargs: (False, {"detail": "boom"})
    )
    alerts_ui._load_managers.clear()

    assert alerts_ui._load_managers() == []


class _RerunTriggered(Exception):
    pass


class _FakeColumn:
    def __init__(self, parent: "_FakeStreamlit", index: int):
        self._parent = parent
        self._index = index

    def selectbox(self, label: str, options, index: int = 0):
        return self._parent._column_selectbox(self._index, label, options, index)

    def date_input(self, label: str, value):
        return self._parent._column_date_input(self._index, label, value)

    def button(self, label: str, key: str | None = None):
        return self._parent.button(label, key=key)

    def write(self, text: str) -> None:
        self._parent.writes.append(text)


class _FakeStreamlit:
    def __init__(
        self,
        *,
        select_values: dict[str, str] | None = None,
        date_range: tuple[date, date] | None = None,
        button_presses: dict[tuple[str, str | None], bool] | None = None,
    ):
        self.select_values = select_values or {
            "event_type": "all",
            "acknowledged": "all",
        }
        self.date_range = date_range or (date.today(), date.today())
        self.button_presses = button_presses or {}
        self.subheaders: list[str] = []
        self.successes: list[str] = []
        self.errors: list[str] = []
        self.infos: list[str] = []
        self.markdowns: list[str] = []
        self.dataframes: list = []
        self.writes: list[str] = []

    def subheader(self, text: str) -> None:
        self.subheaders.append(text)

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [_FakeColumn(self, idx) for idx in range(count)]

    def _column_selectbox(self, _index: int, label: str, _options, _default_index: int):
        return self.select_values[label]

    def _column_date_input(self, _index: int, _label: str, _value):
        return self.date_range

    def button(self, label: str, key: str | None = None):
        return self.button_presses.get((label, key), False)

    def success(self, message: str) -> None:
        self.successes.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def info(self, message: str) -> None:
        self.infos.append(message)

    def markdown(self, text: str) -> None:
        self.markdowns.append(text)

    def dataframe(self, df, use_container_width: bool = False) -> None:
        self.dataframes.append((df, use_container_width))

    def rerun(self) -> None:
        raise _RerunTriggered()


def test_render_alert_inbox_acknowledges_single_alert(monkeypatch):
    fake_st = _FakeStreamlit(
        select_values={"event_type": "all", "acknowledged": "unacknowledged"},
        date_range=(date(2026, 3, 1), date(2026, 3, 2)),
        button_presses={
            ("Acknowledge All", None): False,
            ("Acknowledge", "ack_11"): True,
        },
    )
    monkeypatch.setattr(alerts_ui, "st", fake_st)
    monkeypatch.setattr(
        alerts_ui,
        "_load_alerts",
        lambda *_args, **_kwargs: [
            {
                "alert_id": 11,
                "rule_name": "Rule 11",
                "event_type": "large_delta",
                "payload_json": {"value_usd_gt": 100000},
                "fired_at": "2026-03-02T12:34:56+00:00",
                "acknowledged": False,
            }
        ],
    )
    cleared = {"value": False}
    monkeypatch.setattr(alerts_ui, "_clear_alert_caches", lambda: cleared.update(value=True))

    api_calls: list[tuple[str, str, dict[str, str]]] = []

    def _fake_api_request(method: str, path: str, *, params=None, json_body=None):
        del json_body
        api_calls.append((method, path, params or {}))
        return True, {"acknowledged": True}

    monkeypatch.setattr(alerts_ui, "_api_request", _fake_api_request)

    with pytest.raises(_RerunTriggered):
        alerts_ui._render_alert_inbox()

    assert api_calls == [("POST", "/api/alerts/history/11/acknowledge", {"by": "ui"})]
    assert cleared["value"] is True
    assert len(fake_st.dataframes) == 1


def test_render_alert_inbox_acknowledges_all(monkeypatch):
    fake_st = _FakeStreamlit(
        select_values={"event_type": "all", "acknowledged": "all"},
        date_range=(date(2026, 3, 1), date(2026, 3, 2)),
        button_presses={("Acknowledge All", None): True},
    )
    monkeypatch.setattr(alerts_ui, "st", fake_st)
    monkeypatch.setattr(
        alerts_ui,
        "_load_alerts",
        lambda *_args, **_kwargs: [
            {
                "alert_id": 20,
                "rule_name": "Rule 20",
                "event_type": "new_filing",
                "payload_json": {"filing_type": "13F-HR"},
                "fired_at": "2026-03-01T09:00:00+00:00",
                "acknowledged": False,
            }
        ],
    )
    cleared = {"value": False}
    monkeypatch.setattr(alerts_ui, "_clear_alert_caches", lambda: cleared.update(value=True))

    api_calls: list[tuple[str, str, dict[str, str]]] = []

    def _fake_api_request(method: str, path: str, *, params=None, json_body=None):
        del json_body
        api_calls.append((method, path, params or {}))
        return True, {"acknowledged": 1}

    monkeypatch.setattr(alerts_ui, "_api_request", _fake_api_request)

    with pytest.raises(_RerunTriggered):
        alerts_ui._render_alert_inbox()

    assert api_calls == [("POST", "/api/alerts/history/acknowledge-all", {"by": "ui"})]
    assert fake_st.successes == ["Acknowledged 1 alerts"]
    assert cleared["value"] is True
