from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

import httpx
import pytest
import streamlit as st

from alerts.engine import AlertEngine
from alerts.models import AlertEvent
from ui import alerts as alerts_ui


@pytest.fixture(autouse=True)
def _isolate_alerts_ui_state(monkeypatch):
    """`_load_managers`/`_load_rules`/`_load_alerts` are `st.cache_data`-wrapped, which is a
    process-global cache keyed on arguments -- without a fresh cache, a test that reuses the
    same arguments as an earlier test would silently observe the earlier test's fake data
    instead of its own. `_api_base_url` also reads real environment variables, which must not
    leak in from (or out to) the ambient shell. Autouse so a future test can't forget either."""
    st.cache_data.clear()
    monkeypatch.delenv("ALERTS_API_BASE_URL", raising=False)
    monkeypatch.delenv("API_BASE_URL", raising=False)
    yield


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
    def __init__(self, parent: _FakeStreamlit, index: int):
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


# ---------------------------------------------------------------------------
# _api_base_url: env var precedence + trailing slash normalization
# ---------------------------------------------------------------------------


def test_api_base_url_prefers_alerts_specific_env_var(monkeypatch):
    monkeypatch.setenv("ALERTS_API_BASE_URL", "https://alerts.example.com/")
    monkeypatch.setenv("API_BASE_URL", "https://generic.example.com")

    assert alerts_ui._api_base_url() == "https://alerts.example.com"


def test_api_base_url_falls_back_to_generic_api_base_url(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "https://generic.example.com/")

    assert alerts_ui._api_base_url() == "https://generic.example.com"


def test_api_base_url_defaults_to_localhost_when_unset():
    assert alerts_ui._api_base_url() == "http://localhost:8000"


# ---------------------------------------------------------------------------
# _api_request: the shared HTTP helper every loader/action in this page uses
# ---------------------------------------------------------------------------


class _StubResponse:
    """Fakes the httpx.Response surface _api_request inspects."""

    _NO_JSON = object()

    def __init__(
        self,
        status_code: int,
        *,
        json_payload: Any = _NO_JSON,
        text: str = "",
        content: bytes = b"x",
    ):
        self.status_code = status_code
        self._json_payload = json_payload
        self.text = text
        self.content = content

    def json(self) -> Any:
        if self._json_payload is self._NO_JSON:
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return self._json_payload


def _stub_httpx_client(
    monkeypatch,
    *,
    response: _StubResponse | None = None,
    exc: Exception | None = None,
) -> list[tuple[str, str, dict | None, dict | None]]:
    """Replaces httpx.Client, as seen from ui.alerts, with a fake that records every
    request() call and returns/raises the given canned result."""
    calls: list[tuple[str, str, dict | None, dict | None]] = []

    class _StubClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> _StubClient:
            return self

        def __exit__(self, *exc_info: Any) -> None:
            return None

        def request(self, method: str, url: str, *, params=None, json=None):
            calls.append((method, url, params, json))
            if exc is not None:
                raise exc
            assert response is not None
            return response

    monkeypatch.setattr(alerts_ui.httpx, "Client", _StubClient)
    return calls


def test_api_request_returns_parsed_json_body_on_success(monkeypatch):
    calls = _stub_httpx_client(
        monkeypatch, response=_StubResponse(200, json_payload={"rule_id": 7})
    )

    ok, payload = alerts_ui._api_request("POST", "/api/alerts/rules", json_body={"name": "x"})

    assert (ok, payload) == (True, {"rule_id": 7})
    assert calls == [("POST", "http://localhost:8000/api/alerts/rules", None, {"name": "x"})]


def test_api_request_returns_none_on_success_with_empty_body(monkeypatch):
    # A 204 (or any empty-bodied success) must not call response.json() at all -- an empty
    # body is not valid JSON and would raise.
    _stub_httpx_client(monkeypatch, response=_StubResponse(204, content=b""))

    ok, payload = alerts_ui._api_request("DELETE", "/api/alerts/rules/1")

    assert (ok, payload) == (True, None)


def test_api_request_error_response_returns_parsed_json_detail_at_the_400_boundary(monkeypatch):
    # 400 is the smallest status this must treat as an error (a >, not >=, mutation would
    # let exactly-400 slip through as success).
    _stub_httpx_client(
        monkeypatch, response=_StubResponse(400, json_payload={"detail": "Alert rule not found"})
    )

    ok, payload = alerts_ui._api_request("GET", "/api/alerts/rules/999")

    assert (ok, payload) == (False, {"detail": "Alert rule not found"})


def test_api_request_error_response_falls_back_to_text_when_body_is_not_json(monkeypatch):
    _stub_httpx_client(monkeypatch, response=_StubResponse(502, text="Bad Gateway"))

    ok, payload = alerts_ui._api_request("GET", "/api/alerts/rules")

    assert (ok, payload) == (False, "Bad Gateway")


def test_api_request_catches_transport_errors(monkeypatch):
    _stub_httpx_client(monkeypatch, exc=httpx.ConnectError("connection refused"))

    ok, payload = alerts_ui._api_request("GET", "/api/alerts/rules")

    assert (ok, payload) == (False, "connection refused")


# ---------------------------------------------------------------------------
# _load_rules / _load_alerts: param construction (falsy-but-real filters must survive)
# ---------------------------------------------------------------------------


def test_load_rules_passes_through_provided_filters(monkeypatch):
    calls: list[dict[str, Any] | None] = []

    def _fake(method, path, *, params=None, json_body=None):
        calls.append(params)
        return True, [{"rule_id": 1}]

    monkeypatch.setattr(alerts_ui, "_api_request", _fake)

    result = alerts_ui._load_rules(event_type="large_delta", enabled=False)

    assert result == [{"rule_id": 1}]
    assert calls == [{"event_type": "large_delta", "enabled": False}]


def test_load_rules_omits_filters_when_not_provided(monkeypatch):
    calls: list[dict[str, Any] | None] = []
    monkeypatch.setattr(
        alerts_ui,
        "_api_request",
        lambda method, path, *, params=None, json_body=None: (calls.append(params) or (True, [])),
    )

    alerts_ui._load_rules()

    assert calls == [{}]


def test_load_rules_enabled_false_is_not_confused_with_unset(monkeypatch):
    """enabled=False means "show only disabled rules" -- a real, meaningful filter. If the
    param-building switched from `is not None` to plain truthiness, this filter would
    silently vanish from the request and the endpoint would return ALL rules instead."""
    calls: list[dict[str, Any] | None] = []
    monkeypatch.setattr(
        alerts_ui,
        "_api_request",
        lambda method, path, *, params=None, json_body=None: (calls.append(params) or (True, [])),
    )

    alerts_ui._load_rules(enabled=False)

    assert calls == [{"enabled": False}]


def test_load_rules_returns_empty_list_on_api_failure(monkeypatch):
    monkeypatch.setattr(alerts_ui, "_api_request", lambda *a, **k: (False, "error"))

    assert alerts_ui._load_rules() == []


def test_load_alerts_passes_through_provided_filters(monkeypatch):
    calls: list[dict[str, Any] | None] = []
    monkeypatch.setattr(
        alerts_ui,
        "_api_request",
        lambda method, path, *, params=None, json_body=None: (
            calls.append(params) or (True, [{"alert_id": 1}])
        ),
    )

    result = alerts_ui._load_alerts("2026-01-01T00:00:00", False, "large_delta", 50)

    assert result == [{"alert_id": 1}]
    assert calls == [
        {
            "limit": 50,
            "since": "2026-01-01T00:00:00",
            "acknowledged": False,
            "event_type": "large_delta",
        }
    ]


def test_load_alerts_acknowledged_false_is_a_real_filter_not_a_missing_one(monkeypatch):
    calls: list[dict[str, Any] | None] = []
    monkeypatch.setattr(
        alerts_ui,
        "_api_request",
        lambda method, path, *, params=None, json_body=None: (calls.append(params) or (True, [])),
    )

    alerts_ui._load_alerts(None, False, None, 100)

    assert calls == [{"limit": 100, "acknowledged": False}]


def test_load_alerts_returns_empty_list_on_api_failure(monkeypatch):
    monkeypatch.setattr(alerts_ui, "_api_request", lambda *a, **k: (False, "boom"))

    assert alerts_ui._load_alerts(None, None, None, 10) == []


# ---------------------------------------------------------------------------
# _clear_alert_caches: must actually clear all three loaders' caches
# ---------------------------------------------------------------------------


def test_clear_alert_caches_clears_all_three_loaders(monkeypatch):
    cleared: list[str] = []
    monkeypatch.setattr(alerts_ui._load_managers, "clear", lambda: cleared.append("managers"))
    monkeypatch.setattr(alerts_ui._load_rules, "clear", lambda: cleared.append("rules"))
    monkeypatch.setattr(alerts_ui._load_alerts, "clear", lambda: cleared.append("alerts"))

    alerts_ui._clear_alert_caches()

    assert set(cleared) == {"managers", "rules", "alerts"}


# ---------------------------------------------------------------------------
# _payload_summary: truncation boundary
# ---------------------------------------------------------------------------


def test_payload_summary_joins_keys_without_truncation_when_short():
    assert alerts_ui._payload_summary({"a": 1, "b": "x"}) == "a=1, b=x"


def test_payload_summary_does_not_truncate_at_exactly_max_len():
    text = f"k={'v' * 88}"
    assert len(text) == 90
    assert alerts_ui._payload_summary({"k": "v" * 88}, max_len=90) == text


def test_payload_summary_truncates_with_ellipsis_one_char_over_the_limit():
    text = f"k={'v' * 89}"
    assert len(text) == 91

    result = alerts_ui._payload_summary({"k": "v" * 89}, max_len=90)

    assert result == text[:87] + "..."
    assert len(result) == 90


# ---------------------------------------------------------------------------
# _condition_inputs: per-event-type condition builder
# ---------------------------------------------------------------------------


class _ConditionStreamlit:
    """Returns each widget's own default unless the label is explicitly overridden --
    i.e. simulates a user who leaves everything untouched, or one who picked specific
    values, without needing a real Streamlit form context."""

    def __init__(self, overrides: dict[str, Any] | None = None):
        self.overrides = overrides or {}
        self.captions: list[str] = []
        self.selectbox_calls: list[tuple[str, tuple, int]] = []

    def selectbox(self, label: str, options, index: int = 0):
        self.selectbox_calls.append((label, tuple(options), index))
        if label in self.overrides:
            return self.overrides[label]
        return options[index]

    def text_input(self, label: str, value: str = ""):
        return self.overrides.get(label, value)

    def number_input(
        self, label: str, min_value: float = 0.0, value: float = 0.0, step: float = 0.1
    ):
        return self.overrides.get(label, value)

    def caption(self, text: str) -> None:
        self.captions.append(text)


def test_condition_inputs_activism_event_excludes_all_fields_left_at_their_defaults(monkeypatch):
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    assert alerts_ui._condition_inputs("activism_event") == {}


def test_condition_inputs_activism_event_includes_every_field_once_set(monkeypatch):
    fake_st = _ConditionStreamlit(
        overrides={
            "event subtype": "group_formation",
            "subject_cusip": " 037833100 ",
            "min_ownership_pct": 5.0,
            "min_delta_pct": 2.5,
        }
    )
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    condition = alerts_ui._condition_inputs("activism_event")

    assert condition == {
        "event_type": "group_formation",
        "subject_cusip": "037833100",
        "min_ownership_pct": 5.0,
        "min_delta_pct": 2.5,
    }


def test_condition_inputs_activism_event_known_default_subtype_resolves_its_own_index(monkeypatch):
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    alerts_ui._condition_inputs("activism_event", defaults={"event_type": "stake_increase"})

    _label, options, index = fake_st.selectbox_calls[0]
    assert options[index] == "stake_increase"


def test_condition_inputs_activism_event_unknown_default_subtype_falls_back_to_any(monkeypatch):
    # A condition_json saved before a subtype option was renamed/removed must not crash the
    # edit form -- it should fall back to "any" (index 0) rather than raising ValueError
    # from list.index().
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    condition = alerts_ui._condition_inputs(
        "activism_event", defaults={"event_type": "some_removed_subtype"}
    )

    assert condition == {}
    _label, options, index = fake_st.selectbox_calls[0]
    assert options[index] == "any"


def test_condition_inputs_large_delta_defaults_to_buy_and_100k(monkeypatch):
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    assert alerts_ui._condition_inputs("large_delta") == {
        "delta_type": "buy",
        "value_usd_gt": 100000.0,
    }


def test_condition_inputs_large_delta_returns_the_chosen_type_and_threshold(monkeypatch):
    fake_st = _ConditionStreamlit(overrides={"delta_type": "sell", "value_usd_gt": 250000.0})
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    assert alerts_ui._condition_inputs("large_delta") == {
        "delta_type": "sell",
        "value_usd_gt": 250000.0,
    }


def test_condition_inputs_new_filing_returns_the_chosen_type_and_source(monkeypatch):
    fake_st = _ConditionStreamlit(overrides={"filing_type": "13D", "source": "manual"})
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    assert alerts_ui._condition_inputs("new_filing") == {
        "filing_type": "13D",
        "source": "manual",
    }


def test_condition_inputs_new_filing_unknown_defaults_fall_back_to_first_option(monkeypatch):
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    alerts_ui._condition_inputs(
        "new_filing", defaults={"filing_type": "10-K", "source": "carrier-pigeon"}
    )

    calls = {label: (options, index) for label, options, index in fake_st.selectbox_calls}
    filing_options, filing_index = calls["filing_type"]
    assert filing_options[filing_index] == "13F-HR"
    source_options, source_index = calls["source"]
    assert source_options[source_index] == "sec"


@pytest.mark.parametrize(
    "event_type",
    ["news_spike", "crowded_trade_change", "contrarian_signal", "missing_filing", "etl_failure"],
)
def test_condition_inputs_unconfigured_event_types_return_an_empty_condition(
    event_type, monkeypatch
):
    """THE BUG: these five ALERT_EVENT_TYPES have no dedicated branch above, so they used to
    fall through to a generic "field"/"changed_to" filter that was returned unconditionally.
    No event of any of these five types has ever carried a "field" or "changed_to" payload
    key (see alerts/engine.py::_evaluate_condition's generic payload.get(key) fallback for
    unrecognized condition keys), so a rule built this way could never fire -- silently,
    with the rule appearing to have been created successfully. An empty condition is the
    honest answer: the engine treats {} as "match every occurrence" instead of manufacturing
    a filter that can never be satisfied."""
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)

    condition = alerts_ui._condition_inputs(event_type)

    assert condition == {}
    assert fake_st.captions, "the UI must tell the user there is no filter, rather than stay silent"


def test_condition_inputs_empty_condition_actually_matches_a_real_etl_failure_event(monkeypatch):
    """The concrete consequence of the fix above, at the layer that decides whether an alert
    actually fires: alerts.engine.AlertEngine._evaluate_condition. Before the fix, the
    condition built for an "etl_failure" rule was {"field": "role", "changed_to": ""}, which
    can never match this real payload shape -- exactly what
    etl/evaluation_flow.py::fire_quality_alerts constructs for a genuine ETL failure."""
    fake_st = _ConditionStreamlit()
    monkeypatch.setattr(alerts_ui, "st", fake_st)
    condition = alerts_ui._condition_inputs("etl_failure")

    engine = AlertEngine(sqlite3.connect(":memory:"))
    event = AlertEvent(
        event_type="etl_failure",
        payload={
            "pipeline": "research-assistant-evaluation",
            "failures": {"holdings.unmapped_cusip_rate": "fail"},
            "thresholds": {},
        },
    )

    assert engine._evaluate_condition(condition, event) is True
