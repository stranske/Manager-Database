import datetime as dt
import importlib
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import etl.daily_diff_flow as daily_diff_flow


def test_daily_diff_deployment_uses_env_tz(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == "UTC"
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == "UTC"


def test_daily_diff_deployment_falls_back_to_local_timezone(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    tzinfo = dt.datetime.now().astimezone().tzinfo
    expected = tzinfo.tzname(None) if tzinfo else "UTC"
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == expected
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == expected
