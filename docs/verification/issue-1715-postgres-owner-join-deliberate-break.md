# Issue #1715 PostgreSQL owner-join deliberate-break evidence

This transcript verifies the regression guard added by merged PR #1718. The
exercise ran from `origin/main` at `c270918e` on 2026-09-26 (America/Chicago).

## Baseline

Command:

```text
pytest tests/test_embeddings.py::test_search_documents_postgres_manager_join_uses_document_owner -q
```

Result:

```text
.                                                                        [100%]
```

## Deliberate break

The PostgreSQL query in `embeddings.py` was temporarily changed from the
document-owner join:

```python
LEFT JOIN managers m ON d.manager_id = m.manager_id
```

to the prior faulty behavior, joining `managers` through the requested filter
manager (`manager_id=42`). Running the same command failed as required:

```text
F                                                                        [100%]
=================================== FAILURES ===================================
_______ test_search_documents_postgres_manager_join_uses_document_owner ________

>       assert "LEFT JOIN managers m ON d.manager_id = m.manager_id" in sql
E       AssertionError: assert 'LEFT JOIN managers m ON d.manager_id = m.manager_id' in 'SELECT ... LEFT JOIN managers m ON 42 = m.manager_id ...'

tests/test_embeddings.py:686: AssertionError
=========================== short test summary info ============================
FAILED tests/test_embeddings.py::test_search_documents_postgres_manager_join_uses_document_owner
```

## Revert and verification

After restoring the document-owner join, the exact command passed again:

```text
.                                                                        [100%]
```

`git diff -- embeddings.py` was empty after the restoration, so the deliberate
break did not alter production code.
