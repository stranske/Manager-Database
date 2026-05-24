# LangSmith Fleet Tracing — Manager-Database

Operator runbook for the centralized LangSmith trace, cost, and request
correlation pipeline introduced by stranske/Manager-Database#1048.

The shared schema (`langsmith-fleet/v1`) and dashboard ingestion are owned by
stranske/Workflows#2150; this repo owns the Manager-Database-specific
collection layer.

## What gets emitted

Every call to the chat-api surface emits exactly one `langsmith-fleet/v1` record
through `llm.langsmith_fleet.record_chat_event`. The integration sits inside
`api/chat.py::chat_api`, which means all of the following endpoints emit one
fleet record per request, regardless of whether the chain returns success,
hits a 4xx, or fails with 5xx:

- `POST /api/chat`
- `POST /api/chat/filing-summary`
- `POST /api/chat/holdings-analysis`
- `POST /api/chat/query`
- `POST /api/chat/search`

The feedback endpoint (`POST /api/chat/feedback`) emits a separate
`chat-feedback` correlation record joinable on `run_id` / `domain.response_id`.

Each record includes:

- `schema_version`, `repo`, `surface`, `operation`, `run_id`, `status`,
  `github_issue`, `recorded_at`
- `provider`, `model`, `trace_id`, `trace_url` when available
- `domain.endpoint`, `domain.chain`, `domain.workflow`
  (`filing-summary` | `holdings-analysis` | `nl-query` | `rag-search`)
- `domain.request_id_hash`, `domain.session_id_hash`
- `domain.latency_ms`, `domain.http_status`, `domain.rate_limited`
- `domain.input_tokens`, `domain.output_tokens`, `domain.total_tokens`,
  `domain.cost_usd`, `domain.evaluation_score` (when chain provides them)
- `domain.fallback_state`, `domain.error_state`, `error_category`
  (only on `status="error"`)

`status` is one of `success`, `error`, `fallback`, `no_secret`. The
`no_secret` status is the deterministic CI/no-LangSmith fallback — the
endpoint stays healthy and emits the record, but no remote trace was uploaded.

## Where artifacts land

By default the artifact is written to:

    <repo_root>/artifacts/langsmith/langsmith-fleet.ndjson

Set `MANAGER_DATABASE_LANGSMITH_FLEET_PATH=/absolute/path` to redirect to
another location (used by CI and by the Workflows fleet collector).

Retention is bounded at 2000 lines by default; older lines are pruned
in-place after each append.

## Joining feedback to traces

`POST /api/chat/feedback` accepts a `response_id` derived from the original
chat response (`_response_id_from_trace_url(trace_url)`). The feedback
record's `run_id` matches that `response_id`, so a feedback row joins back to
the original `chat-turn` record by either `run_id` or `domain.response_id`.

## Privacy and safety

Records intentionally never include:

- raw user questions, chat answers, or response payloads
- raw `session_id`, `request_id`, or `trip_id` strings
  (only short SHA-256 hashes — `*_hash` fields)
- `LANGSMITH_API_KEY` or other credentials
- raw HTTP request bodies

If the chain itself populates `trace_url`, the record forwards that URL — it
is a smith.langchain.com run URL, not user content.

## No-secret CI behavior

When `LANGSMITH_API_KEY` is unset:

- `llm.tracing.maybe_enable_langsmith_tracing` returns `False`
- `langsmith_fleet.ensure_langsmith_project_defaults` does not mutate env
- Every emitted record carries `status="no_secret"`
- No network call to LangSmith is attempted
- Tests run deterministically without requiring a key

## When a record is missing

If you expect a record but do not see one:

1. Check `MANAGER_DATABASE_LANGSMITH_FLEET_PATH` and the default artifact
   path — the file is created on first append.
2. Confirm the chat endpoint actually returned (errors before `chat_api`
   was reached, e.g. ASGI lifespan failure, will not emit).
3. Check application logs for `LangSmith fleet emission failed` — this is
   a swallowed exception. Observability errors never break the API.

## Related

- `llm/langsmith_fleet.py` — the centralized emitter
- `llm/tracing.py` — LangSmith tracing context (used by the chains)
- `docs/LANGSMITH_SETUP.md` — project naming and dashboard panels
- `docs/api_design_guidelines.md` — request/response contracts
- `docs/api_rate_limiting.md` — rate limit handling reflected in
  `domain.rate_limited` / `status="fallback"`
