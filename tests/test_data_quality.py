from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import alerts.integration as alert_integration
from alerts.data_quality import (
    DataQualityThresholds,
    build_data_quality_alert_event,
    check_harvest_freshness,
    check_row_count_drop,
    evaluate_daily_quality,
    failing_results,
)


@pytest.fixture
def quality_database(tmp_path, monkeypatch):
    """Real SQL inputs/outbox and only a stub external channel."""
    import sqlite3

    from alerts.channels import DeliveryResult, NotificationChannel
    from alerts.db import ensure_alert_tables
    from etl import data_quality_flow as module

    path = tmp_path / "quality.db"
    now = datetime(2026, 10, 18, 12, tzinfo=UTC)
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE filings(filing_id INTEGER PRIMARY KEY, source TEXT, created_at TEXT);
        CREATE TABLE holdings(cusip TEXT, resolved_ticker TEXT, resolved_figi TEXT,
                              superseded_at TEXT);
        CREATE TABLE identifier_resolution_cache(cusip TEXT PRIMARY KEY, ticker TEXT, figi TEXT);
        CREATE TABLE news_items(created_at TEXT, published_at TEXT);
    """)
    ensure_alert_tables(conn)
    conn.execute("INSERT INTO filings VALUES (1, 'edgar', ?)", (now.isoformat(),))
    conn.execute("INSERT INTO news_items VALUES (?, ?)", (now.isoformat(), now.isoformat()))
    conn.execute("INSERT INTO holdings VALUES ('123456789', 'OK', NULL, NULL)")
    conn.execute("""INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled)
                    VALUES ('Quality', 'etl_failure', '{}', '["email"]', 1)""")
    conn.commit()
    conn.close()
    sent = []

    class Channel(NotificationChannel):
        channel_name = "email"

        async def deliver(self, alert):
            sent.append(alert)
            return DeliveryResult("email", success=True)

    monkeypatch.setattr(module, "connect_db", lambda: sqlite3.connect(path))
    monkeypatch.setattr(
        alert_integration, "build_configured_channels", lambda: {"email": Channel()}
    )
    return module, path, now, sent


def test_data_quality_flow_delivers_etl_failure(quality_database):
    import json
    import sqlite3

    module, path, now, sent = quality_database
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE filings SET created_at = ?", ((now - timedelta(days=3)).isoformat(),))
    result = module.data_quality_flow.fn(now)
    assert result["failures"] == ["edgar-nightly.harvest_freshness"]
    assert len(sent) == 1
    assert sent[0].event.event_type == "etl_failure"
    assert sent[0].event.occurred_at == now.replace(hour=0)
    with sqlite3.connect(path) as conn:
        event_type, payload, channels = conn.execute(
            "SELECT event_type, payload_json, delivered_channels FROM alert_history"
        ).fetchone()
    assert event_type == "etl_failure"
    assert json.loads(payload)["failure_names"] == "edgar-nightly.harvest_freshness"
    assert json.loads(channels) == ["email"]


def test_data_quality_flow_pass_sends_nothing(quality_database):
    import sqlite3

    module, path, now, sent = quality_database
    result = module.data_quality_flow.fn(now)
    assert result["failures"] == []
    assert result["alert_ids"] == []
    assert sent == []
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM alert_history").fetchone()[0] == 0


def test_data_quality_flow_same_day_replay_preserves_payload_and_sends_once(quality_database):
    import sqlite3

    module, path, now, sent = quality_database
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM news_items")
    first = module.data_quality_flow.fn(now)
    # Sources and check ages change on retry; the committed snapshot must not.
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM filings")
    second = module.data_quality_flow.fn(now + timedelta(hours=2))
    assert first["results"] == second["results"]
    assert first["alert_ids"] == second["alert_ids"]
    assert len(sent) == 1
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM alert_history").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM data_quality_runs").fetchone()[0] == 1


def test_data_quality_flow_uses_prior_day_baseline_and_current_identifiers(quality_database):
    import sqlite3

    module, path, now, sent = quality_database
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT INTO holdings VALUES (?, 'OK', NULL, NULL)",
            [(str(n),) for n in range(9)],
        )
    module.data_quality_flow.fn(now)
    tomorrow = now + timedelta(days=1)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM holdings WHERE cusip <> '123456789'")
        conn.execute("UPDATE holdings SET resolved_ticker = NULL")
        conn.execute("UPDATE filings SET created_at = ?", (tomorrow.isoformat(),))
        conn.execute(
            "UPDATE news_items SET created_at = ?, published_at = ?",
            (tomorrow.isoformat(), tomorrow.isoformat()),
        )
    result = module.data_quality_flow.fn(tomorrow)
    assert result["failures"] == ["holdings.row_count_drop", "holdings.unmapped_cusip_rate"]
    observed = {r["name"]: r["observed"] for r in result["results"]}
    assert observed["holdings.row_count_drop"] == 90.0
    assert observed["holdings.unmapped_cusip_rate"] == 100.0
    assert len(sent) == 1


def test_data_quality_flow_cache_mapping_excludes_superseded_holdings(quality_database):
    import sqlite3

    module, path, now, sent = quality_database
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE holdings SET resolved_ticker = NULL")
        conn.execute("INSERT INTO identifier_resolution_cache VALUES ('123456789', NULL, 'FIGI')")
        conn.execute("INSERT INTO holdings VALUES ('RETIRED', NULL, NULL, '2026-01-01')")
    assert module.data_quality_flow.fn(now)["failures"] == []
    assert sent == []


def test_data_quality_flow_delivery_failure_is_warning_with_durable_snapshot(
    quality_database, monkeypatch
):
    import sqlite3

    module, path, now, _ = quality_database
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM news_items")

    def fail(*args, **kwargs):
        raise RuntimeError("channel unavailable")

    monkeypatch.setattr(module, "fire_data_quality_alerts", fail)
    result = module.data_quality_flow.fn(now)
    assert result["failures"]
    assert result["warnings"] == [
        "Alert delivery incomplete; reconcile durable outbox claims before retry"
    ]
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM data_quality_runs").fetchone()[0] == 1


def test_data_quality_flow_deployment_has_daily_schedule(quality_database):
    module, _, _, _ = quality_database
    schedule = module.data_quality_deployment.schedules[0].schedule
    assert schedule.cron == "30 8 * * *"
    assert schedule.timezone == module.LOCAL_TZ


def test_missed_harvest_flags_freshness_failure() -> None:
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)

    result = check_harvest_freshness(
        flow_name="edgar-nightly",
        last_landed_at=None,
        now=now,
        window_minutes=60.0,
    )

    assert result.failed is True
    assert result.name == "edgar-nightly.harvest_freshness"
    assert "no recorded landed_at" in result.message


def test_empty_harvest_is_not_silently_green() -> None:
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)

    result = check_harvest_freshness(
        flow_name="news-hourly",
        last_landed_at=now - timedelta(minutes=5),
        now=now,
        window_minutes=60.0,
        row_count=0,
    )

    assert result.failed is True
    assert result.observed == 0.0
    assert "produced no rows" in result.message


def test_row_count_drop_beyond_tolerance_is_flagged() -> None:
    result = check_row_count_drop(
        table_name="filings",
        current_count=440,
        baseline_count=1000,
        max_drop_pct=25.0,
    )

    assert result.failed is True
    assert result.observed == 56.0
    assert result.threshold == 25.0


def test_env_thresholds_drive_daily_quality(monkeypatch) -> None:
    monkeypatch.setenv("DQ_HARVEST_WINDOW_MINUTES", "30")
    monkeypatch.setenv("DQ_NEWS_MAX_AGE_HOURS", "2")
    monkeypatch.setenv("DQ_ROW_COUNT_DROP_PCT", "10")
    monkeypatch.setenv("DQ_UNMAPPED_CUSIP_RATE_PCT", "3")
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)

    results = evaluate_daily_quality(
        now=now,
        flow_landed_at={"edgar-nightly": now - timedelta(minutes=45)},
        flow_row_counts={"edgar-nightly": 25},
        table_counts={"filings": (80, 100)},
        news_max_published_at=now - timedelta(hours=1),
        unmapped_cusip_rate_pct=4.5,
    )

    failures = failing_results(results)
    assert [failure.name for failure in failures] == [
        "edgar-nightly.harvest_freshness",
        "filings.row_count_drop",
        "holdings.unmapped_cusip_rate",
    ]
    assert DataQualityThresholds.from_env().row_count_drop_pct == 10.0


def test_env_thresholds_reject_non_finite_values(monkeypatch) -> None:
    monkeypatch.setenv("DQ_ROW_COUNT_DROP_PCT", "nan")

    with pytest.raises(ValueError, match="DQ_ROW_COUNT_DROP_PCT must be numeric"):
        DataQualityThresholds.from_env()


def test_build_data_quality_alert_event_uses_existing_etl_failure_channel() -> None:
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
    results = [
        check_harvest_freshness(
            flow_name="edgar-nightly",
            last_landed_at=now - timedelta(days=2),
            now=now,
            window_minutes=60.0,
            row_count=10,
        )
    ]

    event = build_data_quality_alert_event(results, occurred_at=now)

    assert event is not None
    assert event.event_type == "etl_failure"
    assert event.payload["kind"] == "data_quality"
    assert event.payload["failure_count"] == 1
    assert event.payload["failure_names"] == "edgar-nightly.harvest_freshness"
    assert event.payload["failures"][0]["name"] == "edgar-nightly.harvest_freshness"


def test_data_quality_alerts_record_through_integration_wrapper(monkeypatch) -> None:
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
    results = [
        check_harvest_freshness(
            flow_name="edgar-nightly",
            last_landed_at=None,
            now=now,
            window_minutes=60.0,
        )
    ]
    recorded = []

    def fake_record(conn, event):
        recorded.append((conn, event))
        return [42]

    monkeypatch.setattr(alert_integration, "evaluate_and_record_alerts", fake_record)

    alert_ids = alert_integration.evaluate_and_record_data_quality_alerts(
        object(), results, occurred_at=now
    )

    assert alert_ids == [42]
    assert recorded[0][1].event_type == "etl_failure"
    assert recorded[0][1].payload["failure_names"] == "edgar-nightly.harvest_freshness"
