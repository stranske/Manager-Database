# Issue #1731 PostgreSQL document-association deliberate-break evidence

This follow-up preserves the falsification proof requested after merged PR
#1728. The exercise uses the repository's `CI` workflow because no local
PostgreSQL service was available. Each workflow dispatch is bound to the exact
commit SHA named below, so the red and restored runs cannot silently test a
different branch tip.

## Deliberate break

Commit `92a35c05e66fedfc59e6667a5e316d46e2bbbbf4` temporarily changed the
`manager_id` guard in `embeddings.py` so `_store_document_on_connection()`
skipped the `document_managers` association insert. That is the exact behavior
the issue requires the named migration test to reject.

PostgreSQL CI evidence:

- Workflow run: <https://github.com/stranske/Manager-Database/actions/runs/36352304906>
- Exact head: `92a35c05e66fedfc59e6667a5e316d46e2bbbbf4`
- Job/step: `Postgres chain integration` / `Run document manager migration association tests`
- Command: `pytest tests/test_document_managers_migration.py -k document_association -v`

Literal pytest result from the completed job log:

```console
tests/test_document_managers_migration.py FF                             [100%]

FAILED tests/test_document_managers_migration.py::test_document_association_migration_and_search[sqlite] - assert set() == {3}
FAILED tests/test_document_managers_migration.py::test_document_association_migration_and_search[postgres] - assert set() == {3}
======================== 2 failed, 6 warnings in 0.55s =========================
```

Both backends reached the intended assertion and rejected the missing
association. The workflow's broader Python jobs also failed on the intentionally
broken production commit, as expected.

## Revert and restoration

Commit `dbc0d633a455e473400adbdebce1ffb300f5af05` restores the association
insert before this evidence document is merged.

PostgreSQL CI evidence:

- Workflow run: <https://github.com/stranske/Manager-Database/actions/runs/36352345325>
- Exact head: `dbc0d633a455e473400adbdebce1ffb300f5af05`
- Job/step: `Postgres chain integration` / `Run document manager migration association tests`
- Command: `pytest tests/test_document_managers_migration.py -k document_association -v`

Literal pytest result from the completed job log:

```console
tests/test_document_managers_migration.py ..                             [100%]
======================== 2 passed, 6 warnings in 0.61s =========================
```

The restored run also completed both full Python matrices successfully (`1746
passed, 20 skipped` on Python 3.12 and 3.13). The aggregate workflow remained
red only because its separate Docker stack smoke job failed while starting the
MinIO container; the PostgreSQL acceptance job and the exact-head PR Gate were
green.

The same focused command was also run locally after restoration. The SQLite
leg passed and the PostgreSQL leg skipped because
`DOCUMENT_TEST_POSTGRES_URL` was not set:

```console
tests/test_document_managers_migration.py .s                             [100%]
=================== 1 passed, 1 skipped, 7 warnings in 0.62s ===================
```

The local result is supporting evidence only. The two exact-head CI runs above
are the authoritative PostgreSQL falsification and restoration proof.

## Cleanup invariant

The deliberate break and its restoration are both retained in branch history.
The final branch restores the association insert, keeps the disposable-schema
PostgreSQL harness repair, and includes the bounded multi-manager vector-search
fix plus its focused regression coverage. Exact-head Gate run
<https://github.com/stranske/Manager-Database/actions/runs/36352343891> is green.
