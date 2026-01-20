import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
import yaml

from scripts.validate_health_alerts import (
    _ENDPOINT_LABEL_PATTERN,
    _METRIC_THRESHOLD_PATTERN,
    validate_health_alerts,
)


def test_health_alert_warning_threshold_present():
    config_path = Path("monitoring/alerts/health-checks.yml")
    data = yaml.safe_load(config_path.read_text()) or {}
    rules = []
    for group in data.get("groups", []):
        rules.extend(group.get("rules", []))
    rule = {rule.get("alert"): rule for rule in rules}.get("HealthCheckLatencyWarning")
    assert rule is not None
    assert rule.get("labels", {}).get("severity") == "warning"
    expr = rule.get("expr", "")
    assert "health_check_duration_seconds" in expr
    assert _ENDPOINT_LABEL_PATTERN.search(expr)
    comparisons = _METRIC_THRESHOLD_PATTERN.findall(expr)
    assert any(op in (">", ">=") and float(value) >= 0.5 for op, value in comparisons)
    validate_health_alerts(config_path)


def _write_alert_config(tmp_path: Path, expr: str, *, severity: str = "warning") -> Path:
    config_path = tmp_path / "alerts.yml"
    config_path.write_text(
        "\n".join(
            [
                "groups:",
                "  - name: health-checks",
                "    rules:",
                "      - alert: HealthCheckLatencyWarning",
                f"        expr: {expr}",
                "        labels:",
                f"          severity: {severity}",
            ]
        )
    )
    return config_path


def test_health_alert_validator_rejects_non_greater_comparator(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="health"}[5m])) by (le)) <= 0.5',
    )
    with pytest.raises(AssertionError, match="Missing warning alert"):
        validate_health_alerts(config_path)


def test_health_alert_validator_accepts_greater_equal_comparator(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="health"}[5m])) by (le)) >= 0.5',
    )
    # Accept inclusive threshold so >= 500ms is treated as compliant.
    validate_health_alerts(config_path)


def test_health_alert_validator_rejects_threshold_below_500ms(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="health"}[5m])) by (le)) > 0.4',
    )
    with pytest.raises(AssertionError, match="Missing warning alert"):
        validate_health_alerts(config_path)


def test_health_alert_validator_rejects_non_warning_severity(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="/health"}[5m])) by (le)) > 0.6',
        severity="critical",
    )
    with pytest.raises(AssertionError, match="Missing warning alert"):
        validate_health_alerts(config_path)


def test_health_alert_validator_rejects_other_metric_thresholds(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="health"}[5m])) by (le)) > 0.4 or up > 1',
    )
    with pytest.raises(AssertionError, match="Missing warning alert"):
        validate_health_alerts(config_path)


def test_health_alert_validator_accepts_slash_health_endpoint(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="/health"}[5m])) by (le)) > 0.6',
    )
    validate_health_alerts(config_path)


def test_health_alert_validator_accepts_regex_health_endpoint(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint=~"/health"}[5m])) by (le)) > 0.6',
    )
    validate_health_alerts(config_path)


def test_health_alert_validator_accepts_500ms_threshold(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="/health"}[5m])) by (le)) > 0.5',
    )
    validate_health_alerts(config_path)


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
