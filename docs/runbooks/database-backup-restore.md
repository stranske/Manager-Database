# Database Backup And Restore Runbook

Manager-Database stores the production data plane in Postgres. Nightly snapshots
must be written to an encrypted object-store bucket and restore steps must be
validated before any deployment is considered recoverable.

## Snapshot Contract

- Cadence: nightly at 05:15 UTC via `.github/workflows/database-snapshot.yml`.
- Source: `DB_SNAPSHOT_DATABASE_URL`, falling back to `DB_URL` for local runs.
- Destination: `DB_SNAPSHOT_S3_URI`, for example `s3://manager-db-backups/prod`.
- Encryption: `aws s3 cp` uses `--sse aws:kms` when
  `DB_SNAPSHOT_KMS_KEY_ID` is set, otherwise `--sse AES256`.
- Format: `pg_dump --format=custom --no-owner --no-privileges`.
- Retention target: 35 days unless the bucket lifecycle policy is stricter.

The workflow always runs a dry-run contract check without credentials. It only
executes the live backup step when both `DB_SNAPSHOT_DATABASE_URL` and
`DB_SNAPSHOT_S3_URI` are configured as repository secrets.

## Operator Backup Command

```bash
export DB_SNAPSHOT_DATABASE_URL="postgresql://..."
export DB_SNAPSHOT_S3_URI="s3://manager-db-backups/prod"
export DB_SNAPSHOT_KMS_KEY_ID="alias/manager-db-backups"  # optional

python scripts/db_snapshot_restore.py backup --dry-run
python scripts/db_snapshot_restore.py backup
```

The dry run prints a masked JSON plan and does not call `pg_dump` or `aws`.

## Restore Drill

Restore into a disposable database first. Do not point the restore command at
production until the owner has confirmed the target database may be replaced.

```bash
export DB_RESTORE_DATABASE_URL="postgresql://..."

python scripts/db_snapshot_restore.py restore \
  --snapshot-uri "s3://manager-db-backups/prod/manager-database-20260611T051500Z.dump" \
  --dry-run

python scripts/db_snapshot_restore.py restore \
  --snapshot-uri "s3://manager-db-backups/prod/manager-database-20260611T051500Z.dump"
```

The restore command downloads the snapshot and runs
`pg_restore --clean --if-exists --no-owner` against the target database.

## Required Secrets

| Secret | Purpose |
| --- | --- |
| `DB_SNAPSHOT_DATABASE_URL` | Source Postgres database for nightly snapshots. |
| `DB_SNAPSHOT_S3_URI` | Encrypted object-store destination prefix. |
| `DB_SNAPSHOT_KMS_KEY_ID` | Optional KMS key or alias for bucket encryption. |
| AWS credentials | Standard GitHub Actions AWS environment or OIDC role. |

## Recovery Evidence

For each production deployment, keep the latest successful workflow run and one
restore dry-run output in the deployment notes. A quarterly restore drill should
restore the newest snapshot into a disposable database and run the Postgres
schema idempotence verifier plus the `/health/detailed` smoke.
