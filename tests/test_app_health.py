import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import health_app, health_live, health_livez, healthz


def test_health_app_ok():
    payload = health_app()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_health_live_ok():
    payload = health_live()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_healthz_ok():
    # Probe aliases should mirror the base liveness payload.
    payload = healthz()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_health_livez_ok():
    payload = health_livez()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
