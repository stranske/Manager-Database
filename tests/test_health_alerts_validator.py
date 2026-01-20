import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from scripts.validate_health_alerts import validate_health_alerts


def test_health_alert_warning_threshold_present():
    validate_health_alerts(Path("monitoring/alerts/health-checks.yml"))


def _write_alert_config(tmp_path: Path, expr: str) -> Path:
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
                "          severity: warning",
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


def test_health_alert_validator_rejects_threshold_below_500ms(tmp_path: Path):
    config_path = _write_alert_config(
        tmp_path,
        'histogram_quantile(0.95, sum(rate(health_check_duration_seconds_bucket{endpoint="health"}[5m])) by (le)) > 0.4',
    )
    with pytest.raises(AssertionError, match="Missing warning alert"):
        validate_health_alerts(config_path)
