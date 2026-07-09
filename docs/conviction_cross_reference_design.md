# Conviction Cross-Reference Design

Issue: #1404

## Goal

De-bias 13F-only conviction signals by attaching context from insider
transactions, short interest, and options activity to the same securities and
managers that already drive holdings analysis.

The first implementation slice should be deliberately narrow: ingest and expose
Form 4 insider transactions with the edgartools dependency already adopted in
#1323. Short-interest and options feeds need data-source and licensing choices,
so this design records their contracts without making them first-slice
requirements.

## Current Limitation

13F filings capture long equity positions only. A manager can look highly
convicted in 13F data while other market evidence says something different:

- insiders may be selling into the same name;
- short interest may be rising against crowded institutional ownership;
- options flow may show event-driven hedging or leverage that is invisible in
  long-equity holdings.

The repo already has holdings, crowded-trade, conviction, EDGAR adapter, API,
dashboard, and chat surfaces. The cross-reference layer should enrich those
surfaces rather than replace the existing conviction model.

## First-Slice Scope: Form 4

Use Form 4 insider transactions as the first production slice because it has the
lowest integration risk:

- EDGAR is already part of the project direction from #1323.
- Insider transactions are issuer- and security-keyed, so they can attach to
  existing holdings identifiers.
- The data can be surfaced as context before it changes conviction scoring.

The first slice should answer:

- Which held securities also have recent insider buys or sells?
- Are insider transactions directionally aligned with manager conviction?
- Which managers have the most holdings with insider activity since the filing
  date?

## Data Model Sketch

Add an `insider_transactions` table or view:

| Column | Purpose |
| --- | --- |
| `transaction_id` | Stable EDGAR-derived transaction id. |
| `issuer_cik` | Issuer CIK from the filing. |
| `issuer_name` | Issuer display name. |
| `security_identifier` | CUSIP, ticker, FIGI, or repo-standard resolved id. |
| `accession_number` | Source filing accession. |
| `filed_date` | Filing date. |
| `transaction_date` | Transaction date when present. |
| `insider_name` | Reporting owner name. |
| `insider_role` | Officer, director, 10% owner, or normalized role text. |
| `transaction_code` | Form 4 transaction code. |
| `direction` | `buy`, `sell`, `award`, `exercise`, `other`, or `unknown`. |
| `shares` | Shares involved when available. |
| `price` | Transaction price when available. |
| `value_usd` | Shares times price when available. |
| `source_url` | EDGAR filing or archive URL. |
| `ingested_at` | Load timestamp. |

Recommended indexes:

- `(security_identifier, transaction_date DESC)`;
- `(issuer_cik, filed_date DESC)`;
- `(direction, transaction_date DESC)`.

## Cross-Reference View

Add a read model that joins manager holdings to recent insider transactions:

```text
manager_conviction_context(
  manager_id,
  security_identifier,
  report_date,
  conviction_score,
  holding_value_usd,
  insider_buy_count,
  insider_sell_count,
  insider_net_value_usd,
  latest_insider_transaction_date,
  context_flags
)
```

The default lookback should be configurable, with `90` days as the initial
fixture/test default. `context_flags` should be deterministic strings such as
`insider_buy_alignment`, `insider_sell_pressure`, `no_recent_insider_activity`,
and `identifier_unmatched`.

## Conviction Semantics

The Form 4 slice should start as display and explanation context. It must not
silently rewrite conviction scores until there is a separately reviewed scoring
change.

Recommended phases:

1. Add the insider transaction store and cross-reference read model.
2. Show context flags beside conviction results.
3. Add an optional `context_adjusted_conviction_score` only after tests prove
   the adjustment behavior and the owner approves the weighting.

This prevents a new feed from changing ranking semantics before the data quality
and directional rules are proven.

## API Surface

Add a read-only endpoint:

```text
GET /managers/{manager_id}/conviction-context
```

Parameters:

- `report_date` optional, default latest holdings snapshot.
- `lookback_days` default `90`, max `365`.
- `include_transactions` default `false`.
- `limit` default `50`, max `200`.

Response rows should include the held security, conviction score, holding value,
summary insider counts/values, latest transaction date, and context flags.
When `include_transactions=true`, include a bounded list of recent transactions
for each security.

## Dashboard And Chat Surface

The dashboard should add a compact "Conviction context" panel near conviction
and crowded-trade summaries:

- top holdings with insider buy alignment;
- top holdings with insider sell pressure;
- unmatched identifier count;
- a clear empty state when no Form 4 transactions exist in the lookback window.

Chat context should summarize only computed facts from the read model. It should
not infer insider sentiment from raw transaction codes that were not normalized.

## Short-Interest And Options Staging

Short-interest and options are important but should stay staged until data
source decisions are made.

### Short Interest

Required owner/data decisions:

- source: FINRA, exchange feeds, paid vendor, or uploaded file;
- cadence: twice-monthly official settlement data versus more frequent vendor
  estimates;
- identifier normalization path;
- whether the signal is display-only or feeds conviction.

Future table sketch: `short_interest_observations` keyed by security identifier,
settlement date, shares short, days to cover, float percentage, and source.

### Options

Required owner/data decisions:

- source and cost/licensing;
- minimum viable aggregation: put/call open interest, volume, implied
  volatility, or unusual flow;
- how to prevent noisy options activity from dominating long-term conviction.

Future table sketch: `options_activity_observations` keyed by security
identifier, observation date, expiry bucket, put/call side, open interest,
volume, implied volatility, and source.

## Pipeline Contract

First-slice ingestion should be local and deterministic:

```text
ingest_form4_transactions(
  cik_or_tickers=None,
  *,
  since=None,
  until=None,
  source="edgar",
)
```

Expected behavior:

1. Resolve issuer identifiers for held securities.
2. Fetch or read Form 4 filings for the requested date range.
3. Normalize transaction rows and directions.
4. Upsert stable rows into `insider_transactions`.
5. Refresh the `manager_conviction_context` read model.
6. Emit a run summary with filings scanned, rows written, unmatched issuers,
   unmatched securities, and skipped transaction codes.

Networked fetches should be separable from parser tests so CI can run against
fixtures.

## Acceptance Tests For The Implementation Issue

The implementation PR should include:

- fixture parser tests for Form 4 buy, sell, award, and exercise rows;
- identifier matching tests from holdings rows to insider transaction rows;
- SQLite tests for `manager_conviction_context` flag behavior;
- API tests for `GET /managers/{manager_id}/conviction-context`;
- dashboard loader tests for the context panel and empty state;
- a deliberate-break test that flips a transaction direction and proves the
  expected flag assertion fails before restoring it.

## Non-Goals

- No paid short-interest or options vendor integration in the first slice.
- No conviction-score weighting change without a separate owner-approved issue.
- No LLM-derived transaction normalization.
- No replacement of crowded-trade or manager-similarity surfaces.
- No requirement for bitemporal price returns; this is context, not performance
  attribution.
