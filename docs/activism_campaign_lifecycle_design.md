# Activism Campaign Lifecycle Design

Issue: #1405

## Decision Summary

Manager-Database should treat activism as a campaign lifecycle dataset rather
than only a list of 13D/13G filings. The first implementation slice should stay
bounded: link Schedule 13D, 13D/A, 13G, and 13G/A filings into deterministic
campaign timelines using the existing EDGAR and activism tables. Campaign
return estimation, letters, settlement agreements, and full Diligent-style
parity stay deferred until the owner chooses the product ambition and source
coverage.

The first slice should answer:

- Which managers have an active or historical campaign against a target?
- Which filings and amendments make up that campaign?
- What lifecycle status and timeline should the API, dashboard, search, and
  chat surfaces cite?
- Which downstream returns or document-archive work remains explicitly out of
  scope?

## Current Repo Assets

The repo already has the foundations for a campaign layer:

- `etl.activism_flow` fetches Schedule 13D/13G family filings through the
  EDGAR adapter and persists `activism_filings`.
- `etl.activism_detection` derives filing-level activism events into
  `activism_events`.
- `api.activism` exposes filings, events, manager timelines, and active
  campaigns.
- `schema.sql` defines the existing `activism_filings` and `activism_events`
  tables.
- `chains.rag_search` already adds compact activism filing context for manager
  questions.
- `docs/backtest_signal_alpha_design.md` defines the later price/return
  dependency, but that work should not be required for the first campaign
  lifecycle slice.

The gap is that the current data model remains filing-centric. It can report a
13D/A amendment, but it does not give the operator a durable campaign identity,
status, milestone sequence, or target-level lifecycle summary.

## Campaign Identity

Create a campaign entity with a durable `campaign_id` surrogate key. Use a
separate grouping key only while ingesting and deduplicating candidate filings:

```text
campaign_key = (
  manager_id,
  target_identifier,
  opened_filed_date,
)
```

The tuple should use the same field names as the campaign table. It should not
be treated as the persisted identity because target resolution can improve after
backfills. When FIGI or CUSIP resolution changes, update the stored grouping
metadata and data-quality flags while preserving the existing `campaign_id` and
timeline relationships.

Target identity should prefer a resolved security identifier when available:

1. FIGI or future canonical security id;
2. CUSIP from the activism filing;
3. normalized subject company name as a fallback with a data-quality flag.

Managers should resolve through `manager_id`; filings without a matched manager
remain ingestible but should not produce a campaign until manager resolution is
available. The implementation should record skipped rows by reason rather than
silently dropping them.

## Lifecycle Model

Add an additive campaign table or materialized output. Suggested columns:

| Column | Purpose |
| --- | --- |
| `campaign_id` | Stable campaign surrogate key. |
| `manager_id` | Activist or filing manager. |
| `target_identifier` | FIGI, CUSIP, or normalized target fallback. |
| `target_company` | Latest target display name from linked filings. |
| `opened_filed_date` | First linked 13D/13G filing date. |
| `latest_filed_date` | Latest linked filing or amendment date. |
| `status` | `active`, `monitoring`, `closed`, or `unknown`. |
| `initial_ownership_pct` | Ownership percentage from the first filing. |
| `latest_ownership_pct` | Ownership percentage from the latest filing. |
| `filing_count` | Number of linked filings and amendments. |
| `event_count` | Number of linked derived events. |
| `latest_event_type` | Most recent derived event type. |
| `source_forms` | JSON array of linked form types. |
| `data_quality_flags` | JSON array of resolution or parsing caveats. |
| `computed_at` | Campaign materialization timestamp. |

Status should be deterministic:

- `active`: latest filing is Schedule 13D family, or recent 13G family with
  ownership still above the configured threshold.
- `monitoring`: campaign has only passive 13G family filings or stale activity
  without a closing signal.
- `closed`: a termination, exit, ownership drop below threshold, or explicit
  implementation-supported close signal is present.
- `unknown`: required dates or identifiers are missing.

Do not infer board seats, settlement terms, or campaign success unless those
fields are backed by parsed documents or an explicit later data source.

## Timeline Contract

Add a campaign timeline read model built from linked filings and existing
events:

| Field | Purpose |
| --- | --- |
| `campaign_id` | Parent campaign. |
| `event_date` | Filing date or derived event date. |
| `event_type` | `initial_filing`, `amendment`, `threshold_crossed`, etc. |
| `filing_id` | Source filing when applicable. |
| `accession` | EDGAR accession where available. |
| `form_type` | Filing form type. |
| `ownership_pct` | Ownership value on the event row. |
| `summary` | Deterministic sentence assembled from source fields. |
| `source_url` | SEC or stored source URL. |

Timeline rows should sort by `event_date`, then form priority, then accession
or filing id. The summary must be deterministic and cite the source filing; the
LLM layer can summarize later, but it must not invent milestones.

## Pipeline Contract

Add a service function that can run after activism ingestion or on demand:

```text
materialize_activism_campaigns(since=None, stale_after_days=365)
```

Expected behavior:

1. Load eligible `activism_filings` and linked `activism_events`.
2. Normalize manager and target identifiers.
3. Group filings into campaign candidates by manager, target, and opened filing
   date, using `campaign_key` only for candidate grouping.
4. Split a new campaign when a target has a long inactivity gap followed by a
   new initial filing.
5. Compute status, latest ownership, source forms, timeline rows, and quality
   flags.
6. Upsert campaigns and timeline rows transactionally by stable `campaign_id`;
   target-resolution backfills update grouping metadata without re-keying stored
   campaigns.
7. Return a run summary with filings scanned, campaigns written, timeline rows
   written, skipped filings, and skip reasons.

The implementation should be dialect-aware for SQLite and Postgres, following
the existing activism table patterns.

## API And Search Surface

Add read-only campaign endpoints after the materialized tables exist:

```text
GET /api/activism/campaigns
GET /api/activism/campaigns/{campaign_id}
GET /api/activism/campaigns/{campaign_id}/timeline
```

Keep the existing `GET /api/activism/active-campaigns` route as a compatibility
alias during the first campaign-lifecycle rollout. It should read from the new
campaign list model and map `ActiveCampaignResponse` fields from the campaign
summary: manager, target, status, opened/latest dates, latest ownership, filing
count, and data-quality flags. After the list, detail, and timeline endpoints
are stable for one release, a later issue can decide whether to deprecate the
alias; this design does not set a cutoff.

Filters:

- `manager_id`;
- `target_identifier`;
- `status`;
- `filed_from` / `filed_to`;
- `limit`, default `25`, max `100`.

Campaign list responses should include campaign id, manager, target, status,
opened date, latest date, latest ownership, filing count, latest event type,
and data-quality flags. Detail responses can include linked filings and the
timeline.

Search should rank campaigns as manager/target context rather than as a new
global document corpus in the first slice. Existing RAG context can include the
top active campaigns with source URLs once the API contract is stable.

## Dashboard And Chat Surface

The first dashboard slice should add an "Activism campaigns" panel near the
existing activism filings/events views:

- active campaigns by latest filed date;
- manager and target;
- status badge;
- latest ownership;
- linked filing count;
- expandable deterministic timeline.

Chat context should cite campaign rows and source filings only. If no campaign
materialization exists for the selected manager, say that no campaign snapshot
is available instead of falling back to filing-level speculation.

## Deferred Scope

These are deliberately out of scope for the first implementation:

- campaign return estimation;
- holding-period or alpha calculations;
- settlement letters, standstill agreements, or presentation deck archives;
- document OCR and text classification for campaign demands;
- paid vendor parity with Diligent, 13D Monitor, or Insightia;
- owner-facing campaign success scoring.

Campaign return estimation should wait for the backtest and price-source
contracts from the Tier-2 validation track. Letters and agreements should wait
for an explicit owner decision on document sources, retention, and licensing.

## Acceptance Tests For Implementation

The implementation PR should include:

- Unit tests for manager/target grouping, inactivity splitting, and status
  classification.
- SQLite tests that seed 13D, 13D/A, 13G, and 13G/A rows and prove one
  campaign timeline is produced in deterministic order.
- A data-quality test for missing CUSIP or unresolved manager rows.
- API tests for campaign list, detail, and timeline filters.
- A RAG/search context test proving campaign summaries cite source filings.
- A deliberate-break test that changes an amendment date or form type and
  proves the expected timeline order/status assertion fails, then restores it.

## Rollout

1. Add campaign and timeline materialization tables/read models.
2. Implement the service function and run-summary contract.
3. Add read-only API endpoints and tests.
4. Add dashboard panel and RAG context after API tests pass.
5. Revisit campaign returns and document archive only after owner scope is
   explicit.

## Open Risks

- Filing amendments can be noisy; deterministic grouping may need manual
  overrides for edge cases.
- Target identity is weaker without a resolved FIGI/security id.
- Passive 13G activity can look like activism unless status rules are explicit.
- Returns and campaign success require separate price/document decisions and
  should not be implied by lifecycle status.
