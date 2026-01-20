import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.validate_health_alerts import validate_health_alerts


def test_health_alert_warning_threshold_present():
    validate_health_alerts(Path("monitoring/alerts/health-checks.yml"))
