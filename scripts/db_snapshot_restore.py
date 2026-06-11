"""Postgres snapshot and restore helper for Manager-Database deployments.

The default dry-run mode is intentionally credential-free so CI can validate the
backup contract without touching production databases or object storage.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

DEFAULT_PREFIX = "manager-database"
DEFAULT_RETENTION_DAYS = 35


@dataclass(frozen=True)
class BackupPlan:
    action: str
    database_url: str
    snapshot_uri: str
    retention_days: int
    encrypted: bool
    commands: list[list[str]]


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)


def mask_database_url(database_url: str) -> str:
    parsed = urlsplit(database_url)
    if "@" not in parsed.netloc:
        return database_url

    _, host = parsed.netloc.rsplit("@", 1)
    return urlunsplit((parsed.scheme, f"***:***@{host}", parsed.path, parsed.query, parsed.fragment))


def snapshot_uri(bucket_uri: str, *, now: dt.datetime | None = None, prefix: str = DEFAULT_PREFIX) -> str:
    timestamp = (now or _utc_now()).strftime("%Y%m%dT%H%M%SZ")
    return f"{bucket_uri.rstrip('/')}/{prefix}-{timestamp}.dump"


def build_backup_plan(
    *,
    database_url: str,
    bucket_uri: str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    kms_key_id: str | None = None,
    now: dt.datetime | None = None,
) -> BackupPlan:
    uri = snapshot_uri(bucket_uri, now=now)
    pg_dump = [
        "pg_dump",
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        "--dbname",
        mask_database_url(database_url),
    ]
    s3_cp = ["aws", "s3", "cp", "<local-snapshot>", uri, "--only-show-errors"]
    if kms_key_id:
        s3_cp.extend(["--sse", "aws:kms", "--sse-kms-key-id", "<kms-key-id>"])
    else:
        s3_cp.extend(["--sse", "AES256"])

    return BackupPlan(
        action="backup",
        database_url=mask_database_url(database_url),
        snapshot_uri=uri,
        retention_days=retention_days,
        encrypted=True,
        commands=[pg_dump, s3_cp],
    )


def build_restore_plan(*, database_url: str, snapshot: str) -> BackupPlan:
    return BackupPlan(
        action="restore",
        database_url=mask_database_url(database_url),
        snapshot_uri=snapshot,
        retention_days=0,
        encrypted=True,
        commands=[
            ["aws", "s3", "cp", snapshot, "<local-snapshot>", "--only-show-errors"],
            [
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--dbname",
                mask_database_url(database_url),
                "<local-snapshot>",
            ],
        ],
    )


def _print_plan(plan: BackupPlan) -> None:
    print(json.dumps(asdict(plan), indent=2, sort_keys=True))


def _run_backup(database_url: str, bucket_uri: str, kms_key_id: str | None) -> None:
    destination = snapshot_uri(bucket_uri)
    with tempfile.TemporaryDirectory(prefix="mgrdb-snapshot-") as tmpdir:
        snapshot_path = Path(tmpdir) / Path(destination).name
        subprocess.run(
            [
                "pg_dump",
                "--format=custom",
                "--no-owner",
                "--no-privileges",
                "--file",
                str(snapshot_path),
                "--dbname",
                database_url,
            ],
            check=True,
        )
        cmd = ["aws", "s3", "cp", str(snapshot_path), destination, "--only-show-errors"]
        if kms_key_id:
            cmd.extend(["--sse", "aws:kms", "--sse-kms-key-id", kms_key_id])
        else:
            cmd.extend(["--sse", "AES256"])
        subprocess.run(cmd, check=True)
    print(destination)


def _run_restore(database_url: str, snapshot: str) -> None:
    with tempfile.TemporaryDirectory(prefix="mgrdb-restore-") as tmpdir:
        snapshot_path = Path(tmpdir) / Path(snapshot).name
        subprocess.run(["aws", "s3", "cp", snapshot, str(snapshot_path), "--only-show-errors"], check=True)
        subprocess.run(
            [
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--dbname",
                database_url,
                str(snapshot_path),
            ],
            check=True,
        )


def _database_url(env_name: str) -> str:
    value = os.getenv(env_name) or os.getenv("DB_URL")
    if not value:
        raise SystemExit(f"{env_name} or DB_URL is required")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup = subparsers.add_parser("backup", help="Create a Postgres snapshot and upload it to S3")
    backup.add_argument("--dry-run", action="store_true", help="Print the backup plan without executing it")
    backup.add_argument("--database-url-env", default="DB_SNAPSHOT_DATABASE_URL")
    backup.add_argument("--bucket-uri", default=os.getenv("DB_SNAPSHOT_S3_URI"))
    backup.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    backup.add_argument("--kms-key-id", default=os.getenv("DB_SNAPSHOT_KMS_KEY_ID"))

    restore = subparsers.add_parser("restore", help="Restore a Postgres database from an S3 snapshot")
    restore.add_argument("--dry-run", action="store_true", help="Print the restore plan without executing it")
    restore.add_argument("--database-url-env", default="DB_RESTORE_DATABASE_URL")
    restore.add_argument("--snapshot-uri", required=True)

    args = parser.parse_args(argv)

    if args.command == "backup":
        if not args.bucket_uri:
            raise SystemExit("DB_SNAPSHOT_S3_URI or --bucket-uri is required")
        database_url = _database_url(args.database_url_env)
        plan = build_backup_plan(
            database_url=database_url,
            bucket_uri=args.bucket_uri,
            retention_days=args.retention_days,
            kms_key_id=args.kms_key_id,
        )
        if args.dry_run:
            _print_plan(plan)
            return 0
        _run_backup(database_url, args.bucket_uri, args.kms_key_id)
        return 0

    database_url = _database_url(args.database_url_env)
    plan = build_restore_plan(database_url=database_url, snapshot=args.snapshot_uri)
    if args.dry_run:
        _print_plan(plan)
        return 0
    _run_restore(database_url, args.snapshot_uri)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
