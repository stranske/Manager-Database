# Issue #1925 backtest provenance verification

Verified on 2026-11-17 after reviewing commits `854def7` and `d3aaf5b`.
Those commits already implement the nullable position/result provenance fields,
SQLite schema evolution, PostgreSQL bootstrap columns, migration 024, and
amendment-isolation, legacy-NULL, and PostgreSQL DDL parity tests.

This follow-up strengthens the named test to read committed results through a
separate SQLite connection. It also verifies that a failed second detail insert
rolls back the new header and populated provenance while preserving a previously
committed run and its complete result row.

## Task reconciliation

- [x] Add provenance to `ensure_backtest_tables`, `Position`, `_price_position`,
  and `persist_backtest`; verified by the named regression and full test file.
- [x] Add matching nullable columns to `schema.sql` and migration 024; verified
  by SQLite migration readback and PostgreSQL DDL compilation/parity tests.
- [x] Add amendment isolation, legacy NULL readback, and PostgreSQL migration
  parity tests; all tests that do not require a live PostgreSQL server pass.
- [x] Named provenance persistence regression passes.
- [x] Deliberately remove provenance copying, observe failure and NULL persisted
  columns, restore the code, and observe the named regression pass.
- [x] The whole `tests/test_backtest_flow.py` file passes (27 tests).
- [ ] Apply these verified checkboxes and evidence to the PR body. The GitHub
  update tool rejected the request because it requires approval and this run's
  approval policy is `never`. No PR body update was applied.

## Deliberate break and restoration

Temporarily removed these five arguments from the `Position` constructor in
`_price_position`:

```python
source_filing_id=row.get("filing_id"),
source_holding_id=row.get("holding_id", row.get("id")),
event_period=_coerce_date(row.get("_reporting_period")),
source_knowledge_time=_knowledge_time(row),
applied_cutoff=decision_cutoff(decision_date),
```

Ran the named gate:

```console
$ python3 -m pytest tests/test_backtest_flow.py::test_persisted_positions_record_source_provenance -q -m "not slow" --disable-warnings
F                                                                        [100%]
>       assert saved["source_filing_id"] == 10
E       assert None == 10
FAILED tests/test_backtest_flow.py::test_persisted_positions_record_source_provenance - assert None == 10
```

Exit status was 1. Queried that failed test's SQLite database directly for
`source_filing_id`, `source_holding_id`, `event_period`, `source_knowledge_time`,
and `applied_cutoff`; readback was `[(None, None, None, None, None)]`.

Restored all five assignments in a `finally` block before running the same gate:

```console
$ python3 -m pytest tests/test_backtest_flow.py::test_persisted_positions_record_source_provenance -q -m "not slow" --disable-warnings
.                                                                        [100%]
```

Exit status was 0. The production file has no remaining diff from this exercise.

## Full validation

```console
$ python3 -m pytest tests/test_backtest_flow.py -q -m "not slow" -o addopts=
27 passed, 19 warnings in 1.11s
$ python3 -m pytest tests/test_schema_postgres_bootstrap.py -q -m "not slow" -o addopts=
6 passed, 6 skipped, 19 warnings in 0.24s
```

PostgreSQL DDL compilation checks cover legacy, partially provisioned, and fully
provisioned tables and match the nullable, default-free bootstrap column types.
The six live PostgreSQL checks skipped because `MGRDB_PG_TEST_URL` is unset.
Attempting to start an isolated local PostgreSQL instance failed because the
sandbox denied Unix socket creation (`Operation not permitted`). Live PostgreSQL
persistence and catalog parity remain unverified in this run.

The required formatting gate also passed:

```console
$ black --line-length 100 tests/test_backtest_flow.py
1 file left unchanged.
$ BLACK_CACHE_DIR=/tmp/mgrdb-black-cache black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
455 files would be left unchanged.
```

Black's multi-file worker stalled in this sandbox, so all 455 files were checked
individually using the same formatter/options and a writable local cache first.
The required repository-wide command then passed using that cache.

## Handoff blockers

The source and verification changes could not be staged or committed:

```text
fatal: Unable to create '/home/runner/work/Manager-Database/Manager-Database/.git/index.lock': Read-only file system
```

The run's permission policy does not allow escalation. No commit was created.
PR #1928 was verified open with `isDraft=false`; its body remains unchanged
because of the separate GitHub approval restriction described above.
