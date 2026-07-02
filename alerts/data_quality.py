"""Data-quality and freshness checks for scheduled manager-data pipelines."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from alerts.models import AlertEvent
from utils.numeric import parse_finite_float

CheckStatus = Literal["pass", "fail"]
DEFAULT_HARVEST_WINDOW_MINUTES = 26 * 60.0
DEFAULT_NEWS_MAX_AGE_HOURS = 24.0
DEFAULT_ROW_COUNT_DROP_PCT = 50.0
DEFAULT_UNMAPPED_CUSIP_RATE_PCT = 10.0


def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = parse_finite_float(raw, allow_none=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric, got {raw!r}") from exc
    assert parsed is not None
    return parsed


@dataclass(frozen=True, slots=True)
class DataQualityThresholds:
    """Operator-configurable thresholds for standing quality checks."""

    harvest_window_minutes: float = DEFAULT_HARVEST_WINDOW_MINUTES
    news_max_age_hours: float = DEFAULT_NEWS_MAX_AGE_HOURS
    row_count_drop_pct: float = DEFAULT_ROW_COUNT_DROP_PCT
    unmapped_cusip_rate_pct: float = DEFAULT_UNMAPPED_CUSIP_RATE_PCT

    @classmethod
    def from_env(cls) -> DataQualityThresholds:
        return cls(
            harvest_window_minutes=_parse_float_env(
                "DQ_HARVEST_WINDOW_MINUTES", DEFAULT_HARVEST_WINDOW_MINUTES
            ),
            news_max_age_hours=_parse_float_env(
                "DQ_NEWS_MAX_AGE_HOURS", DEFAULT_NEWS_MAX_AGE_HOURS
            ),
            row_count_drop_pct=_parse_float_env(
                "DQ_ROW_COUNT_DROP_PCT", DEFAULT_ROW_COUNT_DROP_PCT
            ),
            unmapped_cusip_rate_pct=_parse_float_env(
                "DQ_UNMAPPED_CUSIP_RATE_PCT", DEFAULT_UNMAPPED_CUSIP_RATE_PCT
            ),
        )


@dataclass(frozen=True, slots=True)
class DataQualityCheckResult:
    name: str
    status: CheckStatus
    observed: float | None
    threshold: float
    message: str

    @property
    def failed(self) -> bool:
        return self.status == "fail"


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def check_harvest_freshness(
    *,
    flow_name: str,
    last_landed_at: datetime | None,
    now: datetime,
    window_minutes: float,
    row_count: int | None = None,
) -> DataQualityCheckResult:
    """Fail when a scheduled harvest is missing, stale, or explicitly empty."""
    current = _as_utc(now) or datetime.now(UTC)
    landed = _as_utc(last_landed_at)
    name = f"{flow_name}.harvest_freshness"

    if landed is None:
        return DataQualityCheckResult(
            name=name,
            status="fail",
            observed=None,
            threshold=window_minutes,
            message=f"{flow_name} has no recorded landed_at timestamp",
        )
    age_minutes = (current - landed).total_seconds() / 60.0
    if age_minutes > window_minutes:
        return DataQualityCheckResult(
            name=name,
            status="fail",
            observed=round(age_minutes, 4),
            threshold=window_minutes,
            message=f"{flow_name} harvest is {age_minutes:.1f} minutes old",
        )
    if row_count is not None and row_count <= 0:
        return DataQualityCheckResult(
            name=name,
            status="fail",
            observed=float(row_count),
            threshold=1.0,
            message=f"{flow_name} harvest landed but produced no rows",
        )
    return DataQualityCheckResult(
        name=name,
        status="pass",
        observed=round(age_minutes, 4),
        threshold=window_minutes,
        message=f"{flow_name} harvest is fresh",
    )


def check_news_freshness(
    *,
    max_published_at: datetime | None,
    now: datetime,
    max_age_hours: float,
) -> DataQualityCheckResult:
    published = _as_utc(max_published_at)
    current = _as_utc(now) or datetime.now(UTC)
    if published is None:
        return DataQualityCheckResult(
            name="news.max_age",
            status="fail",
            observed=None,
            threshold=max_age_hours,
            message="No news timestamp is available",
        )
    age_hours = (current - published).total_seconds() / 3600.0
    status: CheckStatus = "fail" if age_hours > max_age_hours else "pass"
    return DataQualityCheckResult(
        name="news.max_age",
        status=status,
        observed=round(age_hours, 4),
        threshold=max_age_hours,
        message=(
            f"Newest news item is {age_hours:.1f} hours old"
            if status == "fail"
            else "News freshness is within threshold"
        ),
    )


def check_row_count_drop(
    *,
    table_name: str,
    current_count: int,
    baseline_count: int,
    max_drop_pct: float,
) -> DataQualityCheckResult:
    name = f"{table_name}.row_count_drop"
    if baseline_count <= 0:
        return DataQualityCheckResult(
            name=name,
            status="pass",
            observed=0.0,
            threshold=max_drop_pct,
            message=f"{table_name} has no positive baseline yet",
        )
    drop_pct = max(0.0, ((baseline_count - current_count) / baseline_count) * 100.0)
    status: CheckStatus = "fail" if drop_pct > max_drop_pct else "pass"
    return DataQualityCheckResult(
        name=name,
        status=status,
        observed=round(drop_pct, 4),
        threshold=max_drop_pct,
        message=(
            f"{table_name} row count dropped {drop_pct:.1f}% from baseline"
            if status == "fail"
            else f"{table_name} row count is within tolerance"
        ),
    )


def check_unmapped_cusip_rate(
    *,
    unmapped_rate_pct: float,
    max_rate_pct: float,
) -> DataQualityCheckResult:
    status: CheckStatus = "fail" if unmapped_rate_pct > max_rate_pct else "pass"
    return DataQualityCheckResult(
        name="holdings.unmapped_cusip_rate",
        status=status,
        observed=round(unmapped_rate_pct, 4),
        threshold=max_rate_pct,
        message=(
            f"Unmapped CUSIP rate is {unmapped_rate_pct:.1f}%"
            if status == "fail"
            else "Unmapped CUSIP rate is within threshold"
        ),
    )


def failing_results(results: list[DataQualityCheckResult]) -> list[DataQualityCheckResult]:
    return [result for result in results if result.failed]


def build_data_quality_alert_event(
    results: list[DataQualityCheckResult],
    *,
    occurred_at: datetime | None = None,
) -> AlertEvent | None:
    """Build an existing alert-channel event when any data-quality check fails."""
    failures = failing_results(results)
    if not failures:
        return None
    return AlertEvent(
        event_type="etl_failure",
        occurred_at=occurred_at or datetime.now(UTC),
        payload={
            "kind": "data_quality",
            "failure_count": len(failures),
            "failure_names": ", ".join(failure.name for failure in failures),
            "failures": [
                {
                    "name": failure.name,
                    "observed": failure.observed,
                    "threshold": failure.threshold,
                    "message": failure.message,
                }
                for failure in failures
            ],
        },
    )


def evaluate_daily_quality(
    *,
    now: datetime,
    flow_landed_at: dict[str, datetime | None],
    flow_row_counts: dict[str, int],
    table_counts: dict[str, tuple[int, int]],
    news_max_published_at: datetime | None,
    unmapped_cusip_rate_pct: float,
    thresholds: DataQualityThresholds | None = None,
) -> list[DataQualityCheckResult]:
    """Evaluate the standing daily freshness, volume, and identifier checks."""
    config = thresholds or DataQualityThresholds.from_env()
    results: list[DataQualityCheckResult] = []
    for flow_name, landed_at in sorted(flow_landed_at.items()):
        results.append(
            check_harvest_freshness(
                flow_name=flow_name,
                last_landed_at=landed_at,
                now=now,
                window_minutes=config.harvest_window_minutes,
                row_count=flow_row_counts.get(flow_name),
            )
        )
    results.append(
        check_news_freshness(
            max_published_at=news_max_published_at,
            now=now,
            max_age_hours=config.news_max_age_hours,
        )
    )
    for table_name, (current_count, baseline_count) in sorted(table_counts.items()):
        results.append(
            check_row_count_drop(
                table_name=table_name,
                current_count=current_count,
                baseline_count=baseline_count,
                max_drop_pct=config.row_count_drop_pct,
            )
        )
    results.append(
        check_unmapped_cusip_rate(
            unmapped_rate_pct=unmapped_cusip_rate_pct,
            max_rate_pct=config.unmapped_cusip_rate_pct,
        )
    )
    return results
