# Issue 1782 SQL relation and result-bound proof

The chain parses one query in the active dialect, resolves physical tables in each lexical scope, rejects qualified or unsupported relation shapes, and bounds the root result set without changing nested LIMITs. Known-table CTEs and joins remain valid. Raw dialect checks run before SQL generation so SQLite never silently accepts the PostgreSQL-only input that the existing guard rejects.

`1782-deliberate-red.txt` executes the new regression cases with the byte-for-byte pre-change `chains/nl_query.py`. The old chain permits excluded comma/nested relations and returns 250 rows for the nested/offset-count inputs. `1782-restored-green.txt` runs the same cases after byte-identical restoration of the fix. The mutation receipt records both source hashes and exit codes.

`1782-final-validation.txt`: `/opt/anaconda3/bin/python3 -m pytest -o addopts= -q --no-cov tests/test_nl_query_chain.py tests/test_chat_api.py tests/test_chain_postgres_integration.py` completed with **95 passed, 6 skipped**. The real `/api/chat/query` path rejects the excluded relation and returns a 100-row answer for a 251-row input. The six PostgreSQL cases, including the new permitted-join/outer-bound case, require `MGRDB_PG_TEST_URL` and were **not run** locally. No PostgreSQL acceptance claim is made; hosted CI and source-issue acceptance remain required.

The full universal resolver confirmed only the dependency-free `sqlglot==28.10.1` pin was added to the existing frozen dependency graph. Existing lock pins and the intentional app-baseline-kit exclusion remain unchanged. Black, Ruff and `git diff --check` pass.
