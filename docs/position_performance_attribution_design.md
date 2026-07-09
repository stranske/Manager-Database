# Position Performance Attribution Design

Issue: #1402

## Decision Summary

Manager-Database should add position-level performance attribution as a view on
the point-in-time holdings and price plumbing defined by #1400 and #1401, not
as a separate engine. The first slice should answer a narrow question:

```text
After a manager disclosed a position, how did that position perform versus the
configured benchmark, and how much did it contribute to manager skill signals?
```

This design resolves the #1402 methodology decision by starting with
buy-and-hold-since-disclosure attribution. Contribution-style attribution can
come later once normalized portfolio weights, corporate actions, and a licensed
price provider are stable.

## Current Baseline

The repo now has adjacent design contracts for the required dependencies:

- `docs/bitemporal_holdings_design.md` defines visible holdings through an
  as-of knowledge cutoff.
- `docs/backtest_signal_alpha_design.md` defines disclosure-lag entry timing,
  benchmark-aware return rows, and reproducible run records.
- `chains/holdings_analysis.py` and `diff_holdings.py` provide holdings and
  change-analysis surfaces that can feed attribution.
- `api/signals.py` exposes signal-oriented output, but not realized outcomes.

The missing layer is a stable attribution read model that turns position return
rows into manager-level skill evidence.

## Methodology

Use a simple first-slice method:

1. Load a manager's visible holdings through the #1400 as-of contract.
2. For each disclosed position, set
   `entry_date = filed_date + disclosure_lag_days`.
3. Use the #1401 `PriceReturnProvider` contract to fetch total return and
   benchmark return for the attribution window.
4. Compute position excess return as `total_return - benchmark_return`.
5. Compute a manager aggregate from position returns using disclosed position
   weight when available; otherwise use equal weighting and mark the weighting
   method in output.
6. Record missing price or benchmark rows as coverage gaps rather than silently
   dropping them.

The first slice should not attempt factor-model attribution, contribution to
portfolio active return, or sector-neutral skill scores. Those require deeper
market data and benchmark holdings.

## Attribution Windows

Support these initial windows, all measured from the post-lag `entry_date`:

- `entry_to_30d`;
- `entry_to_90d`;
- `entry_to_next_filing`;
- `entry_to_exit_or_trim`, when a later visible filing removes or
  materially reduces the position.

Each window must store the report period, filing date, knowledge time, entry
date, exit date, and benchmark symbol. This prevents look-ahead bias and keeps
the output auditable.

## Data Model Sketch

Use additive tables or materialized outputs:

```sql
position_attribution_runs(
  run_id,
  created_at,
  as_of_cutoff,
  disclosure_lag_days,
  price_source,
  benchmark,
  weighting_method,
  config_json
)

position_attribution_rows(
  run_id,
  manager_id,
  security_id,
  report_period,
  filed_date,
  knowledge_time,
  entry_date,
  exit_date,
  disclosed_weight,
  weighting_method,
  total_return,
  benchmark_return,
  excess_return,
  coverage_status
)

manager_attribution_metrics(
  run_id,
  manager_id,
  window_name,
  metric_name,
  metric_value
)
```

SQLite should store `config_json` as text and dates/timestamps as ISO-8601
strings. Postgres can map the same repository objects to native timestamp/date
types and `jsonb`.

## Service Contract

Add a service boundary that can be used by CLI, API, dashboard, and chat
surfaces:

```python
def run_position_attribution(
    *,
    manager_ids: Sequence[int],
    as_of_cutoff: datetime,
    disclosure_lag_days: int,
    windows: Sequence[str],
    price_provider: PriceReturnProvider,
    benchmark: str,
) -> PositionAttributionResult:
    ...
```

The result should include:

- run metadata;
- per-position attribution rows;
- per-manager metrics;
- coverage summary;
- warnings for equal-weight fallback or missing price data.

## API And Surface Plan

First implementation:

- fixture-backed service and CLI output;
- no operator dashboard controls;
- read-model objects that can be serialized in tests.

Later implementation:

- API endpoint for manager attribution summaries;
- dashboard table showing positions, excess return, and coverage;
- chat context block that can cite a run and explain which positions drove a
  manager skill score.

## Acceptance Tests For Implementation

The follow-up code issue should include:

- a bitemporal fixture proving a position is invisible before knowledge time;
- a disclosure-lag fixture proving entry occurs after filing date;
- benchmark-relative calculations for 30-day and 90-day windows;
- a next-filing window where the exit date comes from the next visible filing;
- equal-weight fallback when disclosed weights are missing;
- coverage-gap output for missing price rows;
- a manager aggregate that is reproducible for the same config and fixture data.

## Non-Goals

- Do not build a separate backtest engine.
- Do not choose a paid price provider in this issue.
- Do not implement factor-model or benchmark-holdings attribution.
- Do not add a dashboard UI until service and fixture behavior are stable.

## Open Risks

- 13F disclosed weights may not match the true portfolio exposure at entry.
- Delisted securities, splits, and mergers can distort early fixture returns.
- Equal weighting is useful for sparse fixture data but should not be presented
  as production-grade manager skill.
- Campaign-driven activism returns should wait until the campaign lifecycle
  model is designed.
