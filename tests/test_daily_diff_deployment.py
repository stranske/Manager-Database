import importlib
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import etl.daily_diff_flow as daily_diff_flow


class _TzWithKey:
    key = "America/New_York"


class _NowWithTz:
    def __init__(self, tzinfo):
        self._tzinfo = tzinfo

    def astimezone(self):
        return type("_Aware", (), {"tzinfo": self._tzinfo})()


class _DateTimeWithTzKey:
    @staticmethod
    def now():
        return _NowWithTz(_TzWithKey())


class _DateTimeWithoutKey:
    @staticmethod
    def now():
        return _NowWithTz(None)


def test_daily_diff_deployment_uses_env_tz(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == "UTC"
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == "UTC"


def test_daily_diff_deployment_falls_back_to_tzinfo_key(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.setattr(daily_diff_flow.dt, "datetime", _DateTimeWithTzKey)
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == "America/New_York"
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == "America/New_York"


def test_daily_diff_deployment_falls_back_to_localtime_symlink(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.setattr(daily_diff_flow.dt, "datetime", _DateTimeWithoutKey)
    monkeypatch.setattr(
        daily_diff_flow.os.path,
        "realpath",
        lambda _: "/usr/share/zoneinfo/Europe/Berlin",
    )
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == "Europe/Berlin"
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == "Europe/Berlin"


def test_daily_diff_deployment_defaults_to_utc_when_unresolvable(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.setattr(daily_diff_flow.dt, "datetime", _DateTimeWithoutKey)
    monkeypatch.setattr(daily_diff_flow.os.path, "realpath", lambda _: "/etc/localtime")
    module = importlib.reload(daily_diff_flow)

    assert module.LOCAL_TZ == "UTC"
    schedule = module.daily_diff_deployment.schedules[0].schedule
    assert schedule.timezone == "UTC"
