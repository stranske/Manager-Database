# Backtest provenance verification (#1925)

Verified on 2026-11-17 against the provenance implementation in `e3a11a2`.

- [x] Source fields are copied from the selected holding onto `Position` and persisted.
- [x] The named amendment-isolation test passes after reloading committed results.
- [x] Removing the five provenance constructor arguments makes the named test fail;
  restoring the source makes it pass again.
- [x] The complete backtest test file passes (38 cases).

## Latest acceptance recheck (2026-11-17)

The named test now also checks a holding first known exactly at the inclusive
UTC decision cutoff, using ingestion's UTC timestamp format. Combined with
the existing position states and amendment dates, it passes all 12 cases:

```sh
python3 -m pytest tests/test_backtest_flow.py::test_persisted_positions_record_source_provenance -q -m "not slow"
python3 -m pytest tests/test_backtest_flow.py -q -m "not slow"
python3 -m pytest tests/test_schema_postgres_bootstrap.py -q -m "not slow"
```

The full backtest file passes 38 cases. The schema file passes four offline
cases and skips five live PostgreSQL cases. Removing the five provenance
constructor arguments made all 12 named cases fail with `assert None == 10`
(pytest exit 1); fresh connections to all 12 distinct databases confirmed five
NULL provenance columns in each result. The source was restored byte for byte
with the SHA256 recorded below, and all 12 cases passed again (exit 0).

The offset-timestamp probe exposed a separate SQLite visibility limitation:
the existing queries compare timestamp text and assume UTC-formatted storage.
This change preserves that query behavior and tests the UTC format used by
ingestion.

All three PR acceptance checkboxes should be checked based on these results
and the prior verification. Updating the PR body, posting the evidence comment,
and adding `needs-human` were each rejected with `MCP tool call requires
approval, but approval policy is never`. Staging the test change also failed:
`Unable to create '.git/index.lock': Read-only file system`. The test change
therefore remains uncommitted for the automation or a human to stage and commit.

Black formatted the changed test file and Ruff passed it. The ordinary
repository-wide Black check stalled; a timed retry was terminated after 30
seconds. The periodic event-loop timer workaround described below completed
the same repository-wide check successfully: 456 files would be unchanged.
PR #1934 was rechecked open with `draft=false`.

## Previous verification

Rechecked in the next iteration on 2026-11-17 after extending the named test
across filled, missing-price, and unresolved-ticker positions. Each position
state is paired with both amendment filing dates, for six cases. The test
reloads ticker, status, skip reason, and all five provenance fields through a
fresh connection, then checks they remain unchanged after amendment ingestion
and replay of the original decision.

The deliberate break made all six cases fail with `assert None == 10` (pytest
exit status 1). Fresh connections to all six failed-run databases confirmed
that all five provenance columns were NULL. Restoring `etl/backtest_flow.py`
byte for byte made all six cases pass (exit status 0), followed by all 32 cases
in the complete file (exit status 0). Both commands included `-m "not slow"`.

Rechecked on 2026-11-17 after extending the named test to cover both an amendment
filed on the decision date but learned later and one filed and learned later.
The named test passes both cases with `-m "not slow"`. Removing the five
provenance arguments makes both cases fail with `assert None == 10` (pytest
exit status 1); a fresh SQLite readback confirms all five columns are NULL in
each case. Restoring the implementation byte for byte makes both cases pass
again (exit status 0). The full test file then passes all 28 cases with
`-m "not slow"` (exit status 0).

The previous iteration's additional amendment case and verification update
were subsequently committed by automation in `e535b0e`. In this iteration,
updating the PR acceptance checkboxes was again rejected because the tool
requires approval and the run's approval policy is `never`. The intended
checkbox reconciliation is all three acceptance criteria complete, based on
the recorded break/restoration evidence and the passing commands above.
The current test changes and verification update remain uncommitted:
`git add tests/test_backtest_flow.py docs/verification/issue-1925-backtest-provenance.md`
failed with `Unable to create '.git/index.lock': Read-only file system`.
The repository-wide Black check again passed for all 456 files using the
periodic-timer wrapper described below, and Ruff passed the changed test file.
Posting the verification comment and adding `needs-human` were also rejected
by the same approval restriction. PR #1934 was rechecked open and ready for review.

## Deliberate break and restoration

Temporarily removed `source_filing_id`, `source_holding_id`, `event_period`,
`source_knowledge_time`, and `applied_cutoff` from the `Position` constructor in
`_price_position`, leaving their nullable defaults in place. Ran:

```text
python3 -m pytest tests/test_backtest_flow.py::test_persisted_positions_record_source_provenance -q
FAILED: assert None == 10
exit status: 1
```

A separate SQLite query against that failed run confirmed the five persisted
columns were `(None, None, None, None, None)`. Restored the source byte for byte;
the SHA256 before and after was
`5e86364610fa90f9a28dee2802acf0789bf094eb6795ac6fd4eff40c56509c46`.
The same named test then passed, as did
`python3 -m pytest tests/test_backtest_flow.py -q`.

## PostgreSQL verification boundary

`python3 -m pytest tests/test_schema_postgres_bootstrap.py -q -m "not slow"`
passes four cases and skips five live PostgreSQL cases. The new offline case
executes migration `025` on SQLite, captures its emitted columns, compiles them
with the PostgreSQL dialect, and compares them with `schema.sql`, including
types, timezone, nullability, and defaults.

Rechecked on November 16, 2026 (America/Chicago) at `5d5c377`.
Both PostgreSQL parity cases passed using psycopg over a Unix socket against
PGlite 0.5.16, and the offline DDL comparison passed in the same run.
The parity cases verified legacy NULLs, committed provenance, and rollback
after a later detail insert fails. A separate connection confirmed the
persisted state for both schema paths.

```sh
# MGRDB_PG_TEST_URL points to the temporary PostgreSQL-protocol socket.
python3 -m pytest tests/test_schema_postgres_bootstrap.py::test_backtest_provenance_postgres_parity tests/test_schema_postgres_bootstrap.py::test_backtest_provenance_migration_postgres_ddl_matches_schema -q
```

Native PostgreSQL 16 remains unverified locally because `initdb` could not
allocate shared memory in this sandbox.

Provenance uses revision `025` because revision `024` already belongs to activism
campaign episodes. Renumbering it to the originally requested `024` would collide
with the existing migration history.

## Formatting

Black formatted the changed Python file, and Ruff passed. The repository-wide
Black check passed with line length 100 and the required exclusions: all 456
files were unchanged. The ordinary invocation stalled waiting for worker
completions in this sandbox. A temporary Python wrapper added a periodic timer
to Black's event loop; formatting rules, file discovery, validation and exit
handling were unchanged. No formatter configuration was changed in the repo.

## Reported CI jobs

On `e3a11a2`, GitHub cancelled both Autofix jobs before either ran a step.
The `Resolve Context` annotation reports a higher-priority waiting request
for `autofix-1934`. The Gate run on that commit passed.
