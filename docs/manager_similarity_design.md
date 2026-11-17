# Manager Similarity And Crowding Design

Issue: #1403

## Goal

Add a manager-to-manager similarity surface that answers:

- Which managers look most like this manager?
- Which positions make them similar?
- Which securities are crowded because many similar managers hold them?
- Where should the API, search, dashboard, and chat context expose the result first?

This is a standalone Tier-2 design. It does not require point-in-time holdings,
prices, returns, or the bitemporal holdings work from #1400. Later versions can
layer event-time snapshots on top, but the first slice should run on the latest
available manager holdings.

## Current Repo Assets

The implementation should reuse existing surfaces instead of adding a parallel
analytics stack:

- `holdings` and `filings` already provide manager-position inputs.
- `etl.conviction_flow.detect_crowded_trades()` already materializes
  security-level crowding into `crowded_trades`.
- `api.signals.query_crowded_trades()` and `ui.dashboard` already expose
  crowded-trade summaries.
- `chains.holdings_analysis.HoldingsAnalysis` already has a
  `cross_manager_overlap` output slot and reads `crowded_trades` into LLM
  context.
- `embeddings.py` already provides document vector search. That should stay
  document-focused for now; manager similarity should start from holdings
  vectors, then optionally blend in document/filing embeddings later.

## Similarity Definition

Use a holdings-overlap score as the default first slice:

```text
similarity_score =
  0.55 * weighted_jaccard_by_value +
  0.25 * plain_jaccard_by_security +
  0.20 * conviction_overlap
```

Where:

- `weighted_jaccard_by_value` compares normalized value weights by CUSIP or
  resolved FIGI when available.
- `plain_jaccard_by_security` prevents small but identical portfolios from being
  hidden by value scale.
- `conviction_overlap` reuses `conviction_scores` when present and otherwise
  contributes `0`.

The score must be normalized to `[0, 1]`, symmetric, and computed only from
positions visible in the selected snapshot. Missing resolved identifiers should
fall back to CUSIP. Rows without any usable security identifier should be
excluded from similarity math but counted in a data-quality field.

## Materialized View Contract

Introduce a `manager_similarity` table or view in the implementation issue:

| Column | Purpose |
| --- | --- |
| `manager_id` | Anchor manager. |
| `peer_manager_id` | Similar manager. |
| `report_date` | Snapshot date used for the comparison. |
| `similarity_score` | Final normalized score. |
| `weighted_overlap_score` | Value-weighted overlap component. |
| `security_overlap_score` | Plain security-overlap component. |
| `conviction_overlap_score` | Conviction overlap component or `0`. |
| `shared_security_count` | Number of shared securities. |
| `anchor_security_count` | Usable securities for the anchor manager. |
| `peer_security_count` | Usable securities for the peer manager. |
| `shared_value_usd` | Shared value exposure where available. |
| `top_shared_holdings` | JSON array of the top shared securities and weights. |
| `computed_at` | Computation timestamp. |

Suggested constraints and indexes:

- Unique key on `(manager_id, peer_manager_id, report_date)`.
- Exclude self-pairs.
- Store both directions for fast API reads, or store canonical ordered pairs and
  expose both directions in the query layer. Prefer both directions unless table
  size becomes a problem.
- Index `(manager_id, report_date, similarity_score DESC)`.

## Pipeline Contract

Add a small ETL function that can run after holdings ingestion or on demand:

```text
refresh_manager_similarity(conn) -> dict[str, Any]
compute_manager_similarity(conn) -> int
```

Expected behavior:

1. Resolve each manager's latest filing (`filings` joined to current `holdings` where `superseded_at IS NULL` when present).
2. Build one normalized holdings set and embedding vector per manager from the latest filing.
3. Replace canonical manager pairs `(manager_id_a < manager_id_b)` in `manager_similarity`.
4. Keep `compute_manager_similarity(conn) -> int` returning `rows_written` for backwards compatibility.
5. Return a JSON-serializable run summary from `refresh_manager_similarity(conn)` with:
   - `rows_scanned`
   - `managers_scanned`
   - `pairs_computed`
   - `rows_written`
   - `skipped_by_reason` (for example `missing_security_identifier` and `empty_union`)
   - `status` (`"updated"` when `rows_written > 0`, otherwise `"empty"`)

### CLI Invocation And Database Prerequisites

Run the refresh locally on demand or immediately after holdings updates:

```bash
python -m etl.manager_similarity_flow
```

An optional SQLite file override can also be passed when `DB_URL` is unset:

```bash
python -m etl.manager_similarity_flow --db-path /path/to/manager_database.db
```

Database environment prerequisites (resolved via `adapters.base.connect_db()` and `config.load_runtime_config()`):

- `DB_URL`: optional PostgreSQL connection URL (`postgres://` or `postgresql://`). When set, takes precedence over `DB_PATH`.
- `DB_PATH`: SQLite database file path used when `DB_URL` is unset (defaults to `manager_database.db`).
- Populated `managers`, `filings`, and `holdings` tables for a non-empty refresh; running against an empty database safely initializes `manager_similarity` and prints an `"empty"` summary JSON line to stdout.

The first implementation should stay deterministic and local. No LLM call is
needed to compute similarity.

## API And Search Surface

Add a read-only API route:

```text
GET /managers/{manager_id}/similar
```

Parameters:

- `report_date` optional; defaults to latest.
- `limit` default `10`, max `50`.
- `min_score` optional filter.
- `include_shared_holdings` default `true`.

Response items should include peer manager id/name, score components, shared
security count, shared value, and the top shared holdings. This route should be
usable by the dashboard, chat context, and tests without duplicating SQL.

Search should not rank similarity rows as standalone global search entities in
the first slice. Instead, manager search results can include a compact
`similar_managers` enrichment once the API contract is stable.

## Dashboard Surface

Add a "Similar managers" panel to the manager dashboard near the existing
conviction and crowded-trades panels:

- top peer managers sorted by `similarity_score`;
- score component badges for value overlap, security overlap, and conviction
  overlap;
- top shared holdings for the selected peer;
- an empty state that names the missing prerequisite, for example no latest
  holdings snapshot or fewer than two shared securities.

The panel should use the existing dashboard data loaders and Streamlit patterns
rather than introducing a separate UI stack.

## Chat Context

Extend `HoldingsAnalysisChain._build_data_context()` only after the API/table is
available. The prompt context should summarize the top similar managers and the
top shared holdings, then let `cross_manager_overlap` refer to both
security-level crowding and manager-level peer similarity.

The LLM must not invent peers. If no similarity rows are available, the context
should say no manager similarity snapshot is available for the selected filters.

## Acceptance Tests For The Implementation Issue

The implementation PR should include:

- Unit tests for weighted Jaccard, plain Jaccard, and blended score behavior.
- SQLite tests that seed three managers and prove the expected top peer order.
- A regression test that rows without CUSIP/resolved identifier are skipped and
  reported.
- API tests for `GET /managers/{manager_id}/similar`.
- Dashboard loader tests for the similar-manager panel.
- A deliberate-break test by changing one shared holding and proving the peer
  order or score assertion fails, then restoring it.

## Rollout

1. Land the table/view, ETL function, and tests.
2. Add the API route and dashboard loader/panel.
3. Add chat context enrichment after API and table tests pass.
4. Consider optional document/filing embedding blend only after holdings-based
   similarity is stable and explainable.

## Non-Goals

- No price/returns attribution in this slice.
- No bitemporal or as-of query dependency in this slice.
- No LLM-generated similarity scores.
- No new vendor data source.
- No global search ranking change until the API contract is proven.
