# Issue #1927 manager similarity refresh deliberate-break evidence

This records the falsification proof for local manager similarity refresh CLI
and API readback in `etl/manager_similarity_flow.main`. The named gate is
`test_local_refresh_command_writes_rows_and_summary`.

Check date: 2026-11-16.

## Named gate on restored code

Interpreter: `/opt/anaconda3/bin/python3`

Command:

```console
$ /opt/anaconda3/bin/python3 -m pytest tests/test_manager_similarity_flow.py::test_local_refresh_command_writes_rows_and_summary -q
.                                                                        [100%]
1 passed
```

File gate:

```console
$ /opt/anaconda3/bin/python3 -m pytest tests/test_manager_similarity_flow.py -q
..............                                                           [100%]
14 passed
```

## Deliberate break

In `etl/manager_similarity_flow.main`, `refresh_manager_similarity(conn)` was
temporarily bypassed after holdings were present. `main` still printed a valid
`"updated"` summary with `rows_written: 3`, but skipped the pairwise inserts.
The first schema-less SQLite invocation still called `refresh_manager_similarity`
so the empty-database coverage on the same test remained exercised.

Precise mutation (removed before merge):

```python
        if (
            isinstance(conn, sqlite3.Connection)
            and table_exists(conn, "holdings")
            and conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0] > 0
        ):
            summary = {
                "rows_scanned": 9,
                "managers_scanned": 3,
                "pairs_computed": 3,
                "rows_written": 3,
                "skipped_by_reason": {"missing_security_identifier": 1},
                "status": "updated",
            }
        else:
            summary = refresh_manager_similarity(conn)
```

Running the named gate failed at API readback because no pairs were written:

```console
$ /opt/anaconda3/bin/python3 -m pytest tests/test_manager_similarity_flow.py::test_local_refresh_command_writes_rows_and_summary -q
F                                                                        [100%]
=================================== FAILURES ===================================
______________ test_local_refresh_command_writes_rows_and_summary ______________
>       assert len(items) == 2
E       assert 0 == 2
E        +  where 0 = len([])

tests/test_manager_similarity_flow.py:444: AssertionError
=========================== short test summary info ============================
FAILED tests/test_manager_similarity_flow.py::test_local_refresh_command_writes_rows_and_summary
```

Empty-row evidence after the broken `main([])` call (stdout claimed
`rows_written: 3`, but the database had no pairs):

```sql
SELECT COUNT(*) FROM manager_similarity;
-- 0
```

`GET /managers/1/similar` returned `{"items": []}`.

## Revert and restoration

`main` was restored to unconditional `summary = refresh_manager_similarity(conn)`.
The named gate and the full `tests/test_manager_similarity_flow.py` file both pass
on the restored head (see above).