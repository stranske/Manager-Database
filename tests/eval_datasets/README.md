# Evaluation datasets

These fixtures back the weekly research-assistant evaluation flow in `etl/evaluation_flow.py`.

Guidelines:
- Keep at least 10 examples per dataset so regressions are not hidden by one-off cases.
- Each entry may include a `run` payload for deterministic offline testing and CI.
- Production evaluation runners can replace the canned `run` payload with live chain output while reusing the same expected fields.
- For deterministic live-chain smoke coverage, run:
  `python -c "from etl.evaluation_flow import live_evaluation_flow; print(live_evaluation_flow())"`.
  This seeds an in-memory SQLite corpus, runs the actual filing summary, NL-to-SQL, and RAG chains with offline fake providers, and evaluates those outputs without external LLM or LangSmith credentials.

Datasets:
- `filing_summary_eval.json`: filing summary quality checks.
- `nl_query_eval.json`: NL-to-SQL correctness and safety checks.
- `rag_search_eval.json`: RAG faithfulness, source attribution, and hallucination checks.

## Golden CI gate

The `golden` pytest marker backs a dedicated, **offline and synthetic** CI job
(`golden` in `.github/workflows/ci.yml`) that runs `pytest -m golden -v`. It
gates two things on every PR, using only in-memory SQLite and the deterministic
fake providers — no `LANGSMITH_API_KEY`, `OPENAI_API_KEY`, or any provider
secret, and no external LLM calls:

- `tests/test_evaluation.py::test_live_eval_meets_thresholds` runs
  `run_live_evaluation_suite()` over the seeded corpus and asserts every metric
  in `summary["metrics"]` meets its bound in
  `etl.evaluation_flow.QUALITY_THRESHOLDS` with no recorded failures. Raising any
  threshold above what the deterministic fixtures score makes this fail.
- `tests/test_diff_holdings.py::test_diff_holdings_golden` asserts the exact
  `diff_holdings` delta list for a fixture exercising all four `delta_type`
  branches (ADD/EXIT/INCREASE/DECREASE), pinning the classification logic.
