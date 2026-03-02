import asyncio
import sqlite3
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _request(method: str, path: str, **kwargs):
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.request(method, path, **kwargs)
    finally:
        await app.router.shutdown()


def test_alerts_router_is_registered():
    route_paths = {route.path for route in app.routes}
    expected_paths = {
        "/api/alerts/rules",
        "/api/alerts/rules/{rule_id}",
        "/api/alerts/history",
        "/api/alerts/unacknowledged/count",
        "/api/alerts/history/{alert_id}/acknowledge",
        "/api/alerts/history/acknowledge-all",
    }
    assert expected_paths.issubset(route_paths)

    openapi = app.openapi()
    for path in expected_paths:
        path_item = openapi["paths"][path]
        assert any("Alerts" in operation.get("tags", []) for operation in path_item.values())


def _create_rule_payload(
    *,
    name: str = "Large Delta Rule",
    event_type: str = "large_delta",
    channels: list[str] | None = None,
    enabled: bool = True,
) -> dict:
    return {
        "name": name,
        "event_type": event_type,
        "condition_json": {"delta_type": "buy", "value_usd_gt": 100000},
        "channels": channels or ["email", "slack"],
        "enabled": enabled,
        "manager_id": None,
    }


def _seed_alert_history(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO alert_history(
                rule_id, rule_name, event_type, payload_json, delivered_channels, acknowledged
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                1,
                "Large Delta Rule",
                "large_delta",
                '{"symbol":"ABC","delta":150000}',
                '["email"]',
                0,
            ),
        )
        conn.execute(
            """INSERT INTO alert_history(
                rule_id, rule_name, event_type, payload_json, delivered_channels, acknowledged
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                1,
                "Large Delta Rule",
                "large_delta",
                '{"symbol":"XYZ","delta":250000}',
                '["slack"]',
                0,
            ),
        )
        conn.execute(
            """INSERT INTO alert_history(
                rule_id, rule_name, event_type, payload_json, delivered_channels, acknowledged
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                2,
                "New Filing Rule",
                "new_filing",
                '{"symbol":"QRS"}',
                '["webhook"]',
                1,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_alert_rule_crud_and_soft_delete(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    create_response = asyncio.run(
        _request("POST", "/api/alerts/rules", json=_create_rule_payload())
    )
    assert create_response.status_code == 201
    created = create_response.json()
    rule_id = created["rule_id"]
    assert created["enabled"] is True
    assert created["event_type"] == "large_delta"

    get_response = asyncio.run(_request("GET", f"/api/alerts/rules/{rule_id}"))
    assert get_response.status_code == 200
    assert get_response.json()["name"] == "Large Delta Rule"

    update_response = asyncio.run(
        _request(
            "PUT",
            f"/api/alerts/rules/{rule_id}",
            json={
                "name": "Updated Rule",
                "channels": ["webhook"],
                "enabled": False,
                "condition_json": {"delta_type": "sell", "value_usd_gt": 500000},
            },
        )
    )
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["name"] == "Updated Rule"
    assert updated["channels"] == ["webhook"]
    assert updated["enabled"] is False

    delete_response = asyncio.run(_request("DELETE", f"/api/alerts/rules/{rule_id}"))
    assert delete_response.status_code == 200
    assert delete_response.json() == {"rule_id": rule_id, "enabled": False}

    post_delete = asyncio.run(_request("GET", f"/api/alerts/rules/{rule_id}"))
    assert post_delete.status_code == 200
    assert post_delete.json()["enabled"] is False


def test_alert_rule_list_filters(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    response_one = asyncio.run(
        _request(
            "POST",
            "/api/alerts/rules",
            json=_create_rule_payload(name="Delta", event_type="large_delta", enabled=True),
        )
    )
    response_two = asyncio.run(
        _request(
            "POST",
            "/api/alerts/rules",
            json=_create_rule_payload(name="Filing", event_type="new_filing", enabled=False),
        )
    )
    assert response_one.status_code == 201
    assert response_two.status_code == 201

    by_event = asyncio.run(
        _request("GET", "/api/alerts/rules", params={"event_type": "large_delta"})
    )
    assert by_event.status_code == 200
    payload = by_event.json()
    assert len(payload) == 1
    assert payload[0]["name"] == "Delta"

    by_enabled = asyncio.run(_request("GET", "/api/alerts/rules", params={"enabled": True}))
    assert by_enabled.status_code == 200
    enabled_rules = by_enabled.json()
    assert len(enabled_rules) == 1
    assert enabled_rules[0]["enabled"] is True


def test_alert_validation_invalid_event_type_rejected(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "POST",
            "/api/alerts/rules",
            json=_create_rule_payload(event_type="bad_event"),
        )
    )

    assert response.status_code == 400
    errors = response.json()["errors"]
    assert errors[0]["field"] == "event_type"


def test_alert_validation_invalid_channels_rejected(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "POST",
            "/api/alerts/rules",
            json=_create_rule_payload(channels=["pagerduty"]),
        )
    )

    assert response.status_code == 400
    errors = response.json()["errors"]
    assert errors[0]["field"] == "channels"


def test_alert_acknowledge_and_count_flow(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    # Ensure alert tables exist by creating at least one rule.
    create_response = asyncio.run(
        _request("POST", "/api/alerts/rules", json=_create_rule_payload())
    )
    assert create_response.status_code == 201
    _seed_alert_history(db_path)

    initial_count = asyncio.run(_request("GET", "/api/alerts/unacknowledged/count"))
    assert initial_count.status_code == 200
    assert initial_count.json() == {"count": 2}

    alerts_response = asyncio.run(
        _request(
            "GET",
            "/api/alerts/history",
            params={"event_type": "large_delta", "acknowledged": False, "limit": 10},
        )
    )
    assert alerts_response.status_code == 200
    alerts = alerts_response.json()
    assert len(alerts) == 2

    alert_id = alerts[0]["alert_id"]
    acknowledge_one = asyncio.run(
        _request("POST", f"/api/alerts/history/{alert_id}/acknowledge", params={"by": "tester"})
    )
    assert acknowledge_one.status_code == 200
    assert acknowledge_one.json()["acknowledged"] is True

    after_single = asyncio.run(_request("GET", "/api/alerts/unacknowledged/count"))
    assert after_single.status_code == 200
    assert after_single.json() == {"count": 1}

    acknowledge_all = asyncio.run(
        _request("POST", "/api/alerts/history/acknowledge-all", params={"by": "tester"})
    )
    assert acknowledge_all.status_code == 200
    assert acknowledge_all.json() == {"acknowledged": 1}

    final_count = asyncio.run(_request("GET", "/api/alerts/unacknowledged/count"))
    assert final_count.status_code == 200
    assert final_count.json() == {"count": 0}
