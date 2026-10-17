# Issue 1787 temporal holdings proof

The shared dialect-aware predicate excludes superseded positions from current reads and applies knowledge and supersession boundaries to historical reads. Diff filing authority survives removal of the last current position. API `context.as_of` accepts ISO dates/timestamps and rejects invalid cutoffs. Historical holdings prompts omit derived current-only tables without a knowledge-time contract. Filing-summary reads also use the shared predicate to satisfy the issue's all-chain-reader scan. Stored audit rows remain intact and legacy schemas retain existing behavior.

Validation used the repository-selected `.venv/bin/python`:

- `python -m pytest tests/test_diff_holdings.py tests/test_holdings_analysis_chain.py tests/test_rag_search_chain.py tests/test_chat_api.py tests/test_bitemporal_holdings.py tests/test_filing_summary_chain.py -q`: raw JUnit/log in `1787-all-final.*`; `test_original_replay_after_amendment_postgres` was skipped because MGRDB_PG_TEST_URL was not set for that run.
- `MGRDB_PG_TEST_URL=<disposable-local-db> python -m pytest tests/test_chain_postgres_integration.py -q`: five passed with no skips, using PostgreSQL 17 + pgvector in a disposable Docker container, never an existing database.
- `python -m pytest tests/test_diff_holdings.py::test_correction_current_and_historical_diff -q`: replaced the actual diff reader with its unfiltered historical read. It failed because the removed position did not produce EXIT. Restored exact original bytes and the same test passed. Raw JUnit/logs and SHA256/exit receipt are included.
- Black check, Ruff and focused Mypy passed. `rg -n "FROM holdings" diff_holdings.py chains/` reviewed: all holdings readers use the shared predicate/selector; filing authority's EXISTS checks stored evidence rather than dropping an all-removed filing.

The PostgreSQL fixture exercises actual current and as-of row selection plus both prompt readers. A deliberately unsupported direct SQL mock was updated to expose its legacy schema; legacy diff tests were not edited. No live provider, deployment or original issue acceptance is inferred from local tests. Hosted CI and exact-head review gates remain with keepalive/closer.
