# Evaluation datasets

These fixtures back the weekly research-assistant evaluation flow in `etl/evaluation_flow.py`.

Guidelines:
- Keep at least 10 examples per dataset so regressions are not hidden by one-off cases.
- Each entry may include a `run` payload for deterministic offline testing and CI.
- Production evaluation runners can replace the canned `run` payload with live chain output while reusing the same expected fields.

Datasets:
- `filing_summary_eval.json`: filing summary quality checks.
- `nl_query_eval.json`: NL-to-SQL correctness and safety checks.
- `rag_search_eval.json`: RAG faithfulness, source attribution, and hallucination checks.
