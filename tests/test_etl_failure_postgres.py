"""Actual ETL-failure entrypoint against the existing disposable PG fixture."""

from alerts.channels import DeliveryResult, NotificationChannel
from tests.test_alert_postgres_integration import (
    pg_conn as pg_conn,
)
from tests.test_alert_postgres_integration import (
    pg_url as pg_url,
)
from tests.test_alert_postgres_integration import (
    psycopg_module as psycopg_module,
)


def test_etl_quality_failure_dispatch_postgres_replay(request, monkeypatch):
    fixture = request.getfixturevalue("pg_conn")
    import json

    import alerts.integration as integration
    from etl.evaluation_flow import fire_quality_alerts

    conn = fixture.conn
    conn.execute(
        "UPDATE alert_rules SET event_type=%s, condition_json=%s::jsonb, channels=%s, manager_id=NULL WHERE rule_id=%s",
        ("etl_failure", "{}", ["email", "slack"], fixture.rule_id),
    )
    conn.commit()
    sends = []

    class Channel(NotificationChannel):
        def __init__(self, name):
            self.channel_name = name

        async def deliver(self, alert):
            sends.append(self.channel_name)
            return DeliveryResult(channel=self.channel_name, success=True)

    monkeypatch.setattr(
        integration,
        "build_configured_channels",
        lambda: {name: Channel(name) for name in ["email", "slack"]},
    )
    summary = {"failures": {"sql_safety": 0.0}}
    assert fire_quality_alerts.fn(summary, db_conn=conn) == 1
    assert fire_quality_alerts.fn(json.loads(json.dumps(summary)), db_conn=conn) == 1
    assert sends == ["email", "slack"]
    assert conn.execute("SELECT delivered_channels FROM alert_history").fetchone()[0] == [
        "email",
        "slack",
    ]
    assert conn.execute("SELECT COUNT(*) FROM alert_delivery_attempts").fetchone()[0] == 2
