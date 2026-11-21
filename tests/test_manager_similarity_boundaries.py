"""Verify the similarity endpoint and its dashboard presentation."""

import asyncio
import sqlite3
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from api import managers
from api.chat import app
from ui import dashboard


def _api_get(url, params=None):
    async def request():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=cast(Any, app)), base_url="http://test"
        ) as client:
            return await client.get(url, params=params)

    return asyncio.run(request())


def _get(basis, limit=1):
    return _api_get("/managers/1/similar", params={"basis": basis, "limit": limit})


@pytest.mark.parametrize("basis", ["jaccard", "cosine"])
@pytest.mark.parametrize("invalid_column", ["jaccard", "cosine"])
def test_similarity_discards_nonfinite_rows_before_limit(
    tmp_path, monkeypatch, basis, invalid_column
):
    path = tmp_path / "similarity.db"
    monkeypatch.setenv("DB_PATH", str(path))
    monkeypatch.delenv("DB_URL", raising=False)
    with sqlite3.connect(path) as conn:
        managers._ensure_manager_table(conn)
        conn.executemany(
            "INSERT INTO managers(id, name) VALUES (?, ?)",
            [(1, "One"), (2, "Two"), (3, "Three"), (4, "Four")],
        )
        managers.ensure_manager_similarity_table(conn)
        invalid = {"jaccard": 0.99, "cosine": 0.99}
        invalid[invalid_column] = float("inf")
        conn.executemany(
            "INSERT INTO manager_similarity "
            "(manager_id_a, manager_id_b, jaccard, cosine, overlap_count, union_count) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, 2, invalid["jaccard"], invalid["cosine"], 9, 10),
                (1, 3, 0.5, 0.6, 3, 5),
                (1, 4, 0.7, 0.4, 7, 10),
            ],
        )
    # Both finite peers survive filtering; their order depends on the selected basis.
    three = {
        "manager_id": 3,
        "basis": basis,
        "score": 0.6 if basis == "cosine" else 0.5,
        "jaccard": 0.5,
        "cosine": 0.6,
        "overlap_count": 3,
        "union_count": 5,
    }
    four = {
        "manager_id": 4,
        "basis": basis,
        "score": 0.4 if basis == "cosine" else 0.7,
        "jaccard": 0.7,
        "cosine": 0.4,
        "overlap_count": 7,
        "union_count": 10,
    }
    expected = [three, four] if basis == "cosine" else [four, three]
    response = _get(basis)
    assert response.status_code == 200
    assert response.json() == {"items": expected[:1]}
    response = _get(basis, limit=2)
    assert response.status_code == 200
    assert response.json() == {"items": expected}

    # A negative infinity in the selected metric sorts last. A nonbinding limit
    # ensures the invalid row cannot escape detection just by ranking below peers.
    invalid[invalid_column] = float("-inf")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE manager_similarity SET jaccard = ?, cosine = ? "
            "WHERE manager_id_a = ? AND manager_id_b = ?",
            (invalid["jaccard"], invalid["cosine"], 1, 2),
        )
    for limit in (1, 3):
        response = _get(basis, limit=limit)
        assert response.status_code == 200
        assert response.json() == {"items": expected[:limit]}


@pytest.mark.parametrize("stage", ["connect", "query"])
def test_similarity_database_outage_is_sanitized_and_connection_closed(monkeypatch, stage):
    failure = sqlite3.OperationalError("private /secret/manager.db unavailable")
    connect = Mock()
    conn = Mock(spec=sqlite3.Connection)
    if stage == "connect":
        connect.side_effect = failure
    else:
        connect.return_value = conn
        conn.execute.side_effect = failure
    monkeypatch.setattr(managers, "connect_db", connect)
    monkeypatch.setattr(managers, "_ensure_manager_table", Mock())
    monkeypatch.setattr(managers, "_manager_id_column", Mock(return_value="id"))
    response = _get("jaccard")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    connect.assert_called_once_with()
    if stage == "connect":
        conn.close.assert_not_called()
    else:
        conn.execute.assert_called_once_with("SELECT 1 FROM managers WHERE id = ?", (1,))
        conn.close.assert_called_once_with()

        # Also fail the results query after the manager lookup succeeds. Cleanup
        # and sanitization must hold at either query boundary.
        connect.reset_mock()
        conn.reset_mock()
        manager_lookup = Mock()
        manager_lookup.fetchone.return_value = (1,)
        conn.execute.side_effect = [manager_lookup, failure]
        ensure_similarity = Mock()
        monkeypatch.setattr(managers, "ensure_manager_similarity_table", ensure_similarity)

        response = _get("cosine")
        assert response.status_code == 503
        assert response.json() == {"detail": "Database unavailable"}
        connect.assert_called_once_with()
        manager_lookup.fetchone.assert_called_once_with()
        ensure_similarity.assert_called_once_with(conn)
        assert conn.execute.call_count == 2
        query, parameters = conn.execute.call_args.args
        assert "FROM manager_similarity" in query
        assert "ORDER BY cosine DESC" in query
        assert parameters == (1, 1, 1)
        conn.close.assert_called_once_with()


@pytest.mark.parametrize("layout", ["tabs", "tabless"])
def test_dashboard_shows_peer_similarity_in_both_layouts(tmp_path, monkeypatch, layout):
    path = tmp_path / "dashboard.db"
    monkeypatch.setenv("DB_PATH", str(path))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("UI_OFFLINE", raising=False)
    monkeypatch.setenv("API_BASE_URL", "http://testserver")
    monkeypatch.setenv("MANAGER_SIMILARITY_DEFAULT_BASIS", "cosine")
    with sqlite3.connect(path) as conn:
        managers._ensure_manager_table(conn)
        conn.executemany(
            "INSERT INTO managers(id, name) VALUES (?, ?)",
            [(1, "Selected"), (2, "Peer Capital"), (3, "No Peers")],
        )
        managers.ensure_manager_similarity_table(conn)
        conn.execute(
            "INSERT INTO manager_similarity "
            "(manager_id_a, manager_id_b, jaccard, cosine, overlap_count, union_count, computed_at) "
            "VALUES (1, 2, 0.5, 0.75, 2, 4, '2000-01-01 00:00:00')"
        )

    http_client = httpx.Client
    requests = []

    def send(request):
        response = _api_get(str(request.url))
        requests.append((request.method, request.url.path, response.status_code))
        return httpx.Response(response.status_code, content=response.content)

    transport = httpx.MockTransport(send)
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: http_client(transport=transport, **kwargs)
    )
    monkeypatch.setattr(dashboard, "require_login", lambda: True)
    monkeypatch.setattr(dashboard, "load_unacknowledged_alert_count", lambda: 0)
    monkeypatch.setattr(dashboard, "load_managers", dashboard.load_managers.__wrapped__)
    conviction = {
        "name_of_issuer": "Conviction Issuer",
        "cusip": "000000001",
        "conviction_pct": 60.0,
        "portfolio_weight": 0.6,
        "value_usd": 2000.0,
    }
    monkeypatch.setattr(
        dashboard,
        "load_manager_conviction_scores",
        lambda *_args, **_kwargs: pd.DataFrame([conviction]),
    )
    if layout == "tabless":

        class TablessStreamlit:
            def __getattr__(self, name):
                if name == "tabs":
                    raise AttributeError(name)
                return getattr(st, name)

        monkeypatch.setattr(dashboard, "st", TablessStreamlit())

    view = AppTest.from_file("ui/app.py", default_timeout=10).run()
    assert not view.exception
    assert view.selectbox(key="selected_manager_id").value == "all"
    assert all(panel.label != "Similar Managers" for panel in view.expander)
    assert not requests

    def render(manager_id):
        view.selectbox(key="selected_manager_id").set_value(manager_id)
        view.run()
        assert not view.exception
        if layout == "tabs":
            assert [tab.label for tab in view.tabs] == [
                "Portfolio",
                "Activism",
                "Conviction & Signals",
            ]
            portfolio, _, conviction_card = view.tabs
            signals_info = conviction_card.info
        else:
            assert not view.tabs
            portfolio = view
            cards = {panel.label: panel for panel in view.expander}
            conviction_card = cards["Top Convictions"]
            signals_info = [*cards["Crowded Trades"].info, *cards["Contrarian Alerts"].info]

        tables = [element.value for element in conviction_card.dataframe]
        assert [table.to_dict("records") for table in tables if "conviction_pct" in table] == [
            [conviction]
        ]
        assert len(conviction_card.get("vega_lite_chart")) == 1
        assert {
            "No crowded trades overlap found for the selected manager.",
            "No contrarian signals found for the selected manager.",
        } <= {info.value for info in signals_info}
        panels = [panel for panel in view.expander if panel.label == "Similar Managers"]
        assert len(panels) == 1, "Expected one Similar Managers panel after choosing a manager"
        assert panels[0] in portfolio.expander
        assert not panels[0].get("vega_lite_chart")
        return panels[0]

    panel = render(1)
    assert len(panel.dataframe) == 1
    assert panel.dataframe[0].value.to_dict("records") == [
        {
            "Manager": "Peer Capital",
            "Manager ID": 2,
            "Score": 0.75,
            "Basis": "Cosine",
            "Overlap Count": 2,
            "Union Count": 4,
        }
    ]
    assert panel.caption[0].value == "As-of date unavailable. Similarity data may be stale."

    panel = render(3)
    assert not panel.dataframe
    assert panel.info[0].value == "No similarity results available for the selected manager."

    monkeypatch.setattr(managers, "connect_db", Mock(side_effect=sqlite3.OperationalError))
    panel = render(1)
    assert not panel.dataframe
    assert panel.warning[0].value == "Similarity data is currently unavailable."
    assert requests == [
        ("GET", "/managers/1/similar", 200),
        ("GET", "/managers/3/similar", 200),
        ("GET", "/managers/1/similar", 503),
    ]
