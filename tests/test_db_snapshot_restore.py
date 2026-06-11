import datetime as dt
import json
import subprocess

import pytest
import yaml  # type: ignore[import-untyped]

from scripts import db_snapshot_restore


def test_backup_plan_masks_credentials_and_uses_encrypted_snapshot():
    plan = db_snapshot_restore.build_backup_plan(
        database_url="postgresql://manager:secret@db.internal:5432/manager",
        bucket_uri="s3://manager-db-backups/prod",
        kms_key_id="alias/manager-db",
        now=dt.datetime(2026, 6, 11, 5, 15, tzinfo=dt.UTC),
    )

    assert plan.snapshot_uri == "s3://manager-db-backups/prod/manager-database-20260611T051500Z.dump"
    assert plan.database_url == "postgresql://***:***@db.internal:5432/manager"
    assert plan.encrypted is True
    assert "--format=custom" in plan.commands[0]
    assert "--sse" in plan.commands[1]
    assert "aws:kms" in plan.commands[1]
    assert "secret" not in json.dumps(plan.commands)


def test_backup_dry_run_does_not_execute_clients(monkeypatch, capsys):
    calls: list[list[str]] = []
    monkeypatch.setenv("DB_SNAPSHOT_DATABASE_URL", "postgresql://user:pw@db:5432/manager")
    monkeypatch.setenv("DB_SNAPSHOT_S3_URI", "s3://manager-db-backups/prod")
    monkeypatch.setattr(subprocess, "run", lambda command, **_: calls.append(command))

    assert db_snapshot_restore.main(["backup", "--dry-run"]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["action"] == "backup"
    assert output["database_url"] == "postgresql://***:***@db:5432/manager"
    assert output["snapshot_uri"].startswith("s3://manager-db-backups/prod/manager-database-")
    assert calls == []


def test_restore_dry_run_plans_pg_restore_without_clients(monkeypatch, capsys):
    calls: list[list[str]] = []
    monkeypatch.setenv("DB_RESTORE_DATABASE_URL", "postgresql://restore:pw@db:5432/restore")
    monkeypatch.setattr(subprocess, "run", lambda command, **_: calls.append(command))

    assert (
        db_snapshot_restore.main(
            [
                "restore",
                "--snapshot-uri",
                "s3://manager-db-backups/prod/manager-database-20260611T051500Z.dump",
                "--dry-run",
            ]
        )
        == 0
    )

    output = json.loads(capsys.readouterr().out)
    assert output["action"] == "restore"
    assert output["commands"][1][0] == "pg_restore"
    assert "--clean" in output["commands"][1]
    assert calls == []


def test_backup_requires_bucket_uri(monkeypatch):
    monkeypatch.setenv("DB_SNAPSHOT_DATABASE_URL", "postgresql://user:pw@db:5432/manager")
    monkeypatch.delenv("DB_SNAPSHOT_S3_URI", raising=False)

    with pytest.raises(SystemExit, match="DB_SNAPSHOT_S3_URI"):
        db_snapshot_restore.main(["backup", "--dry-run"])


def test_database_snapshot_workflow_runs_dry_run_and_conditional_live_backup():
    workflow = yaml.safe_load(open(".github/workflows/database-snapshot.yml", encoding="utf-8"))
    triggers = workflow[True]
    job = workflow["jobs"]["postgres-snapshot"]
    steps = job["steps"]

    assert triggers["schedule"] == [{"cron": "15 5 * * *"}]
    assert triggers["workflow_dispatch"] is None
    assert job["env"]["DB_SNAPSHOT_DATABASE_URL"] == "${{ secrets.DB_SNAPSHOT_DATABASE_URL }}"
    assert any(
        step.get("run") == "python scripts/db_snapshot_restore.py backup --dry-run"
        for step in steps
    )

    live_step = next(
        step for step in steps if step.get("name") == "Run encrypted Postgres snapshot"
    )
    assert "DB_SNAPSHOT_DATABASE_URL" in live_step["if"]
    assert live_step["run"] == "python scripts/db_snapshot_restore.py backup"
