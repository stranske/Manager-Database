# Backtest provenance verification (#1925)

Verified on 2026-11-17 against the provenance implementation in `e3a11a2`.

- [x] Source fields are copied from the selected holding onto `Position` and persisted.
- [x] The named amendment-isolation test passes after reloading committed results.
- [x] Removing the five provenance constructor arguments makes the named test fail;
  restoring the source makes it pass again.
- [x] The complete backtest test file passes (28 cases).

Rechecked on 2026-11-17 after extending the named test to cover both an amendment
filed on the decision date but learned later and one filed and learned later.
The named test passes both cases with `-m "not slow"`. Removing the five
provenance arguments makes both cases fail with `assert None == 10` (pytest
exit status 1); a fresh SQLite readback confirms all five columns are NULL in
each case. Restoring the implementation byte for byte makes both cases pass
again (exit status 0). The full test file then passes all 28 cases with
`-m "not slow"` (exit status 0).

The additional test case and this verification update remain uncommitted:
`git add` cannot create `.git/index.lock` because the Git directory is mounted
read-only. Updating the PR acceptance checkboxes was also rejected because the
tool requires approval and the run's approval policy is `never`. PR #1934 was
verified open and ready for review before that rejected update.

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
