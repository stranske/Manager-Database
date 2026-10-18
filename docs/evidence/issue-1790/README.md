# Scheduled data-quality delivery proof

The scope tests use real SQLite source tables, rules, alert history and durable outbox claims. Only the external email channel is stubbed. No external notification or live deployment was performed.

`/opt/anaconda3/bin/python3 -m pytest tests/test_data_quality.py -k data_quality_flow -q -rA`: seven flow/schedule cases pass. Temporarily replacing the dispatch return in `fire_data_quality_alerts` with `return []` causes the three unchanged delivery/replay/baseline regressions to fail (exit1). Byte-identical restoration returns the complete data-quality module to green (14 tests). Raw RED/restored GREEN logs are adjacent. Initial fixture used an unsupported channel name; corrected to a stub implementation of the supported email channel before causal proof.

Focused mypy passes for the two changed production modules. The combined data-quality, alert integration/channels/engine and daily-diff validation passes195 tests; raw focused-final log is adjacent. PostgreSQL, external sends and a running Prefect deployment are unverified. The module defines a scheduled deployment; operators serve/register it using the existing Prefect worker. Landing timestamps refer to persisted records, not proof that an empty harvest ran.

Review recovery: invalid persisted landing/publication timestamps now yield missing-data freshness failures instead of aborting the scheduled quality flow. The new unchanged regression fails on the old parser; restored production passes all15data-quality tests. Focused mypy/Black/Ruff/diff pass. Raw review RED/GREEN logs are adjacent.
