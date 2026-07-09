# Point-In-Time Holdings Design

Issue: #1400

## Decision Summary

Manager-Database should implement full bitemporal holdings history, not a lighter
overwrite-plus-view model. The holdings model must preserve both:

- `event_time`: the reporting period or filing-effective date the row describes.
- `knowledge_time`: the timestamp when this system learned, corrected, or
  superseded that observation.

Rows are append-only. Corrections, late 13F amendments, parser repairs, and
backfills add new versions instead of mutating prior observations. The initial
implementation should keep all knowledge-time versions indefinitely; compaction
can only be added later as a derived archive after point-in-time queries remain
reproducible.

The schema must stay dialect-aware across the repo's two supported execution
surfaces:

- SQLite for local CI, fixtures, and the offline demo.
- Postgres for the container/API deployment path.

## Current Baseline

The current canonical tables are `filings` and `holdings` in `schema.sql`.
`diff_holdings.py` already chooses one authoritative filing per manager-period,
preferring amendments when present, while leaving raw EDGAR filing rows
queryable. `chains/holdings_analysis.py` reads holdings through `filings` and
uses `COALESCE(f.period_end, f.filed_date)` as the report date.

That is sufficient for current latest-state views, but it cannot answer:

- What did we believe manager X held as of query date Q?
- Did a backtest use only information known before the rebalance date?
- Which parser or filing correction changed an earlier period after the fact?

## Proposed Data Model

Add bitemporal columns to both filing-level and position-level rows:

```sql
-- filings
event_time date
knowledge_time timestamptz
supersedes_filing_id bigint null
observation_source text

-- holdings
event_time date
knowledge_time timestamptz
supersedes_holding_id bigint null
observation_source text
```

Column meanings:

- `event_time` is normally `period_end`, falling back to `filed_date` only when
  the source truly lacks a reporting period.
- `knowledge_time` is the system observation time. New ingestion uses the
  ingest timestamp. Backfill stamps existing rows with `filed_date` as the best
  available proxy and records `observation_source = 'backfill:filed_date'`.
- `supersedes_*_id` is optional lineage for explicit parser corrections or
  amended filing replacement. It is not required to query latest as-of state,
  but it makes audit trails and review diffs easier.
- `observation_source` names why the version exists, such as `edgar-ingest`,
  `13f-amendment`, `parser-repair`, or `backfill:filed_date`.

Indexes needed for the implementation issue:

```sql
CREATE INDEX idx_filings_manager_event_knowledge
    ON filings (manager_id, event_time, knowledge_time DESC, filing_id DESC);

CREATE INDEX idx_holdings_filing_event_knowledge
    ON holdings (filing_id, event_time, knowledge_time DESC, holding_id DESC);

CREATE INDEX idx_holdings_cusip_event_knowledge
    ON holdings (cusip, event_time, knowledge_time DESC, holding_id DESC);
```

Postgres migrations should use `timestamptz`; SQLite fixtures can store ISO-8601
text timestamps and rely on lexical ordering.

## As-Of Query Contract

The query helper should expose this shape:

```python
load_holdings_as_of(
    conn,
    *,
    manager_id: int,
    as_of: date | datetime,
    event_time: date | None = None,
    cusips: Sequence[str] | None = None,
) -> list[dict[str, Any]]
```

Semantics:

1. Normalize `as_of` before comparing it with `knowledge_time`: callers should
   pass a timezone-aware `datetime`; bare `date` values are interpreted as
   midnight UTC at the start of that date. SQLite stores the normalized value as
   ISO-8601 UTC text, while Postgres compares the same instant as `timestamptz`.
2. Filter rows to `knowledge_time <= as_of`.
3. If `event_time` is provided, use only that reporting period; otherwise choose
   the latest event period whose knowledge time is visible by `as_of`.
4. For each logical position key, pick the latest visible version ordered by
   `(knowledge_time, holding_id)`.
5. A logical position key is `(manager_id, event_time, cusip)` when CUSIP is
   present; otherwise use `(manager_id, event_time, name_of_issuer,
   resolved_ticker, resolved_figi)` so unresolved fixture rows remain queryable.
6. Return filing metadata alongside each holding so downstream prompts and
   backtests can display both the reporting period and the knowledge cutoff.

Postgres can express the last-visible row with `DISTINCT ON` or
`row_number() over (...)`. SQLite should use a correlated `NOT EXISTS` or window
function only after the supported SQLite version is confirmed in CI. Prefer a
dialect helper instead of embedding one SQL string in every caller.

## Migration Sketch

The implementation issue should use an additive migration:

1. Add nullable `event_time`, `knowledge_time`, `supersedes_*_id`, and
   `observation_source` columns.
2. Backfill existing `filings.event_time` from `period_end` or `filed_date`.
3. Backfill existing `filings.knowledge_time` from `filed_date`, with a final
   fallback to `created_at` or the migration timestamp for legacy fixtures.
4. Backfill `holdings.event_time` and `holdings.knowledge_time` by joining each
   row's filing.
5. Add indexes after the backfill.
6. Make new ingestion write event and knowledge time explicitly.
7. Add the as-of helper, then move `diff_holdings.py`, holdings analysis, and
   backtest code paths to the helper when they need point-in-time behavior.

Do not add non-null constraints until local SQLite fixtures, Postgres schema
bootstrap, and Alembic migrations all write the new fields. The first code PR
should include parity tests for SQLite and Postgres placeholders rather than
assuming one backend.

## Acceptance Tests For Implementation

The follow-up implementation issue should include these tests:

- Existing rows backfill with `knowledge_time = filed_date` and keep current
  latest holdings behavior unchanged.
- A late amendment for the same manager, period, and CUSIP is invisible before
  its `knowledge_time` and visible after it.
- A parser repair appends a corrected holding version without deleting the
  original row.
- `diff_holdings.py` and at least one chain/query surface can request an as-of
  cutoff without seeing future knowledge.
- SQLite and Postgres query builders use the correct placeholder and timestamp
  representation for their backend.

## Open Risks

- Historical backfill uses `filed_date` as a proxy for knowledge time. That is
  conservative for many filings, but it is still inferred data and should be
  labeled as such in audit output.
- Queries that group unresolved positions need a stable fallback key until
  identifier resolution is complete.
- Existing demos and fixtures may omit `period_end`; the migration must preserve
  their current behavior by falling back to `filed_date`.
