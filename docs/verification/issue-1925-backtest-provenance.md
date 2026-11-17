# Backtest provenance verification (#1925)

Verified on 2026-11-17 against the provenance implementation in `e3a11a2`.

- [x] Source fields are copied from the selected holding onto `Position` and persisted.
- [x] The named amendment-isolation test passes after reloading committed results.
- [x] Removing the five provenance constructor arguments makes the named test fail;
  restoring the source makes it pass again.
- [x] The complete backtest test file passes (27 cases).

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

The live parity cases now check pre-migration legacy rows, successful provenance
writes, and rollback after a later detail insert fails, with readback from a
separate connection. These live checks remain unverified here: the sandbox
denies both TCP and Unix socket creation, so the temporary PostgreSQL server
could not start. Run them with `MGRDB_PG_TEST_URL` in a socket-enabled environment.

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

## Delivery blocker

These follow-up tests and this record remain uncommitted: Git cannot create
`.git/index.lock` because this run mounts `.git` read-only. Updating the PR's
acceptance checkboxes and adding `needs-human` also failed: the GitHub tools
require approval, while this run's approval policy is `never`. No PR metadata
was changed. A writable checkout is needed to commit the local changes to the
existing PR branch; the verification above does not establish live PostgreSQL
transaction behavior.
