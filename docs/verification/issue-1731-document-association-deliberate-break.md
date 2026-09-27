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

Literal pytest output from the completed job log (Postgres chain integration step):

```console
============================= test session starts ==============================
platform linux -- Python 3.14.7, pytest-9.1.1, pluggy-1.6.0
collected 2 items

tests/test_document_managers_migration.py FF                             [100%]

=================================== FAILURES ===================================
____________ test_document_association_migration_and_search[sqlite] ____________
>               assert {hit["doc_id"] for hit in hits} == expected
E               assert set() == {3}

___________ test_document_association_migration_and_search[postgres] ___________
E           sqlalchemy.exc.IntegrityError: (psycopg.errors.ForeignKeyViolation) insert or update on table "document_managers" violates foreign key constraint "document_managers_doc_id_fkey"
E           [SQL: INSERT INTO document_managers (doc_id, manager_id) SELECT doc_id, manager_id FROM documents WHERE manager_id IS NOT NULL ON CONFLICT (doc_id, manager_id) DO NOTHING]

=========================== short test summary info ============================
FAILED tests/test_document_managers_migration.py::test_document_association_migration_and_search[sqlite] - assert set() == {3}
FAILED tests/test_document_managers_migration.py::test_document_association_migration_and_search[postgres] - sqlalchemy.exc.IntegrityError: (psycopg.errors.ForeignKeyViolation) insert or update on table "document_managers" violates foreign key constraint "document_managers_doc_id_fkey"
=================== 2 failed, 6 warnings in 0.77s ====================
```

The SQLite failure is the deliberate-break signal: manager 1 returned no owned
hits, so the result omitted document ID 3. Manager 3 is separately expected to
return an empty set.
The PostgreSQL leg failed earlier during migration `022` backfill in the isolated CI schema
before the association-insert guard was exercised.

## Revert and restoration

Commit `211e117c84bd2cc29cb4603be56a844fe212b221` restores the original
association insert before this evidence document is merged.

PostgreSQL CI evidence:

- Workflow run: <https://github.com/stranske/Manager-Database/actions/runs/36343489519>
- Exact head: `211e117c84bd2cc29cb4603be56a844fe212b221`
- Job/step: `Postgres chain integration` / `Run document manager migration association tests`
- Command: `pytest tests/test_document_managers_migration.py -k document_association -v`

Literal pytest output from the completed job log (Postgres chain integration step):

```console
============================= test session starts ==============================
platform linux -- Python 3.14.7, pytest-9.1.1, pluggy-1.6.0
collected 2 items

tests/test_document_managers_migration.py .F                             [100%]

=================================== FAILURES ===================================
___________ test_document_association_migration_and_search[postgres] ___________
E           sqlalchemy.exc.IntegrityError: (psycopg.errors.ForeignKeyViolation) insert or update on table "document_managers" violates foreign key constraint "document_managers_doc_id_fkey"
E           [SQL: INSERT INTO document_managers (doc_id, manager_id) SELECT doc_id, manager_id FROM documents WHERE manager_id IS NOT NULL ON CONFLICT (doc_id, manager_id) DO NOTHING]

=========================== short test summary info ============================
FAILED tests/test_document_managers_migration.py::test_document_association_migration_and_search[postgres] - sqlalchemy.exc.IntegrityError: (psycopg.errors.ForeignKeyViolation) insert or update on table "document_managers" violates foreign key constraint "document_managers_doc_id_fkey"
=================== 1 failed, 1 passed, 6 warnings in 0.77s ====================
```

Restoration is confirmed on the SQLite leg (`.` in `.F`). PostgreSQL acceptance is still
blocked on the same migration-backfill FK failure in the disposable-schema CI harness, not
on a restored production-code regression (`git diff origin/main -- embeddings.py` remains empty).

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
