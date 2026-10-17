# Durable large-delta delivery proof

Completion definition: `docs/alert-delivery-semantics.md`. Daily diff rows commit before the existing durable dispatcher receives stable report-date/payload events. Persistence-only paths no longer report unattempted external channels as delivered. Delivery failures warn without erasing domain writes; per-channel claims preserve at-most-once attempts. PostgreSQL Decimal amounts are normalized for the same JSON payload as SQLite.

- `python -m pytest tests/test_daily_diff.py tests/test_alert_integration.py -q`: 38 passed. Actual flow entry tests assert committed rows/claims visible through another connection before stub email/Slack delivery, identical replay suppression, independent changed-diff delivery, channel exceptions, failed outcome commits and unconfigured channels. Persistence-only external delivery accounting is tested.
- `MGRDB_PG_TEST_URL=<disposable-db> python -m pytest tests/test_alert_postgres_integration.py -q`: all 13 passed, including the actual daily producer under normal, channel-failure and failed-outcome-commit conditions plus existing durable-claim scenarios. This used disposable PostgreSQL17/pgvector, never an existing database.
- Replaced the actual producer's dispatcher call with the old persistence-only call: named `[none]` delivery regression failed because no email was sent. Restored identical source bytes and the same test passed. Raw JUnit/logs and source-SHA256/exit receipt retained. Machine-specific executable path is normalized only in the public JSON receipt; original remains in local automation storage.
- Focused Mypy, Black and Ruff passed. No external email/Slack send, provider verdict, hosted green or original source acceptance is asserted; channels were stubs.

This PR owns source1785 large-delta delivery. Source1789 retains the separate ETL-failure entrypoint; it may reuse these actual producer controls rather than duplicate this implementation.
