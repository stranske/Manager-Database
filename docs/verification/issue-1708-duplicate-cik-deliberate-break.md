# Issue #1708 duplicate-CIK deliberate-break evidence

Issue #1721 records the missing falsification proof for the duplicate-CIK guard
delivered by merged PR #1712. The check below ran from commit
`c270918e6fb668b3a48e4586eb6596abeb1727f5`, a direct child of `origin/main`,
on 2026-09-26.

## Named gate on the unmodified baseline

Command:

```console
$ pytest tests/test_manager_api.py::test_create_manager_rejects_duplicate_cik_on_sqlite_bootstrap -q
.                                                                        [100%]
1 passed
```

The test proves that the first request succeeds, the second request with the
same CIK returns HTTP 409, and SQLite retains exactly one matching row.

## Deliberate break

The temporary mutation in `api/managers.py` changed
`_ensure_universe_schema(conn)` to `_ensure_manager_table(conn)` and disabled
the `_manager_exists_for_cik(conn, payload.cik)` pre-insert branch. This removes
both protections whose absence caused the original SQLite bootstrap defect.

Running the same named gate then failed for the expected reason:

```console
$ pytest tests/test_manager_api.py::test_create_manager_rejects_duplicate_cik_on_sqlite_bootstrap -q
F                                                                        [100%]
=================================== FAILURES ===================================
________ test_create_manager_rejects_duplicate_cik_on_sqlite_bootstrap _________

    duplicate = asyncio.run(_post_manager({**payload, "name": "Duplicate Manager Name"}))
>   assert duplicate.status_code == 409
E   assert 201 == 409
E    +  where 201 = <Response [201 Created]>.status_code

tests/test_manager_api.py:140: AssertionError
1 failed
```

The duplicate request returned 201, reproducing the original acceptance
failure and demonstrating that the named test detects loss of the guard.

## Revert and restoration

The temporary mutation was reverted before this evidence file was committed.
The identical command passed again:

```console
$ pytest tests/test_manager_api.py::test_create_manager_rejects_duplicate_cik_on_sqlite_bootstrap -q
.                                                                        [100%]
1 passed
```

Cleanup proof:

```console
$ git diff --exit-code HEAD -- api/managers.py
$ echo $?
0
```

No production or test file differs from the baseline after the deliberate
break; this follow-up adds only the durable transcript.
