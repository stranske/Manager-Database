# Issue #1731 PostgreSQL document-association deliberate-break evidence

This follow-up preserves the falsification proof requested after merged PR
#1728. The exercise uses the repository's `CI` workflow because no local
PostgreSQL service was available. Each workflow dispatch is bound to the exact
commit SHA named below, so the red and restored runs cannot silently test a
different branch tip.

## Deliberate break

Commit `4f507dcd3241fa95a032369200ec4a50861ae342` temporarily changed the
`manager_id` guard in `embeddings.py` so `_store_document_on_connection()`
skipped the `document_managers` association insert. That is the exact behavior
the issue requires the named migration test to reject.

PostgreSQL CI evidence:

- Workflow run: <https://github.com/stranske/Manager-Database/actions/runs/36343416204>
- Exact head: `4f507dcd3241fa95a032369200ec4a50861ae342`
- Job/step: `Postgres chain integration` / `Run document manager migration association tests`
- Command: `pytest tests/test_document_managers_migration.py -k document_association -v`

The literal failing pytest block will be copied here from the completed job log;
the run was still in progress when the evidence branch was first handed to CI.

## Revert and restoration

Commit `211e117c84bd2cc29cb4603be56a844fe212b221` restores the original
association insert before this evidence document is merged.

PostgreSQL CI evidence:

- Workflow run: <https://github.com/stranske/Manager-Database/actions/runs/36343489519>
- Exact head: `211e117c84bd2cc29cb4603be56a844fe212b221`
- Job/step: `Postgres chain integration` / `Run document manager migration association tests`
- Command: `pytest tests/test_document_managers_migration.py -k document_association -v`

The literal passing pytest block will be copied here from the completed job log;
the run was still in progress when the restored branch was first handed to CI.

The same command was also run locally after restoration. The SQLite leg passed
and the PostgreSQL leg skipped because `DOCUMENT_TEST_POSTGRES_URL` was not set:

```console
tests/test_document_managers_migration.py .s                             [100%]
=================== 1 passed, 1 skipped, 7 warnings in 0.62s ===================
```

This local result is supporting cleanup evidence only; the two exact-head CI
runs above are the acceptance evidence for PostgreSQL.

## Cleanup invariant

The deliberate break and its restoration are both retained in branch history,
while the final diff against `origin/main` contains no production-code change.
Only this durable transcript remains in the pull-request diff.
