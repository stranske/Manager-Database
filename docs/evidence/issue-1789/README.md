# Actual ETL-failure dispatcher proof

Completion contract: `docs/alert-delivery-semantics.md`. The current quality-failure production function is `etl/evaluation_flow.py:fire_quality_alerts` (the issue's daily-diff source anchor is stale). It commits observations before dispatch and retains `alert_observed_at` across production/serialized summaries, so a replay reuses claims while a fresh evaluation remains independent. Existing channel/event vocabulary and connection cleanup are preserved.

`python -m pytest tests/test_daily_diff.py tests/test_evaluation.py tests/test_alert_integration.py -q`: all 63 passed. Actual entrypoint test remains in test_daily_diff.py: separate-connection visibility of committed observation and claims, one stub email/Slack send each, serialized replay suppression, correct delivered channels, fresh-observation independence. Existing mock tests now target the actual dispatch function and validate the added UTC summary field without weakening cleanup assertions.

Real disposable PostgreSQL suite: 11 passed, including the ETL producer. The ETL test was then separated into `tests/test_etl_failure_postgres.py` to avoid overlapping PR1810's test edits, and that exact test passed again. Its fixtures reuse the established disposable database contract. No actual email/Slack message was sent.

Causal proof: disconnected the real producer at its current evaluation_flow.py location, preserving the old persistence-only call. The named daily-diff regression failed because sends=[]; byte-identical restoration passed. Raw JUnit/logs and source hash receipt retained. Focused Mypy, Black and Ruff passed. Exact machine path remains only in local automation receipts.

Large-delta behavior is controlled by source1785/PR1810, with exact-head/hash receipt in large-delta-control.json. This separate PR does not duplicate that implementation; its delivery remains a dependency of combined original acceptance. Hosted CI, provider comparison and source closure are not inferred from these local tests.
