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
