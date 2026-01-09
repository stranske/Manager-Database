import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import health_app


def test_health_app_ok():
    payload = health_app()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
