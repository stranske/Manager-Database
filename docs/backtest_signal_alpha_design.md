# Backtest And Signal Alpha Harness Design

Issue: #1401

## Decision Summary

Manager-Database should add an internal signal-validation harness before it
adds an operator-facing backtester UI. The first slice should prove whether
manager conviction and activism signals work under a point-in-time data
contract, then publish stable metrics that later UI, API, and LLM surfaces can
reuse.

The harness depends on the bitemporal holdings contract from #1400:

- holdings are selected through an as-of knowledge cutoff;
- portfolio entry occurs after disclosure lag, not at the report date;
- backtest output records the event period and the knowledge cutoff used for
  every simulated position.

For the initial implementation, use an internal fixture/offline price contract
instead of adding a paid market-data dependency. A provider adapter can be added
later once the owner chooses licensing and redistribution terms.

## Current Baseline

The repo already has pieces that can feed a validation harness:

- `diff_holdings.py` reconciles manager filing changes and amendment selection.
- `chains/holdings_analysis.py` computes portfolio overlap and holdings-level
  analytics.
- `api/signals.py` and `api/activism.py` expose signal and campaign-oriented
  surfaces.
- `docs/bitemporal_holdings_design.md` defines the future as-of holdings helper
  that prevents look-ahead bias.

Those surfaces do not currently answer whether a signal would have worked
historically. They describe conviction or activism, but they do not replay a
strategy with a knowledge cutoff, disclosure lag, benchmark, and realized
return window.

## Strategy Contract

The first strategy should be intentionally narrow:

```text
high_conviction_new_buys
```

Inputs:

- manager universe;
- rebalance dates;
- as-of holdings cutoff;
- disclosure lag in calendar days;
- minimum position weight or portfolio-rank threshold;
- optional activism event filter;
- benchmark symbol or benchmark return series.

Selection semantics:

1. Load visible holdings through the #1400 as-of helper.
2. Compare the current visible period with the prior visible period for each
   manager.
3. Select new or materially increased positions that cross the conviction
   threshold.
4. Enter at `knowledge_time + disclosure_lag`.
5. Hold until the configured horizon or next rebalance.
6. Attribute returns at the position, manager, period, and strategy levels.

The initial implementation should keep the strategy declarative so later
signals can be added without rewriting the engine.

## Price Source Contract

Do not hard-code a vendor dependency in the first PR. Define a narrow adapter:

```python
class PriceReturnProvider(Protocol):
    def returns(
        self,
        identifiers: Sequence[str],
        *,
        start: date,
        end: date,
        benchmark: str | None = None,
    ) -> ReturnFrame:
        ...
```

The fixture provider should load checked-in test data with:

- security identifier;
- start and end date;
- total return;
- benchmark return;
- currency;
- source tag.

The production adapter remains an owner decision because provider cost,
licensing, and redistribution rules affect whether generated reports can be
shared outside the local operator environment.

## Metrics

The harness should emit these first-slice metrics:

- absolute return;
- benchmark-relative return;
- hit rate;
- average position return;
- volatility;
- Sharpe ratio when enough periods exist;
- max drawdown;
- turnover;
- coverage rate for missing price data;
- attribution by manager, signal bucket, and holding period.

Every metric row should include `strategy_id`, `run_id`, `as_of_cutoff`,
`disclosure_lag_days`, `price_source`, and `benchmark` so results are
reproducible and comparable across runs.

## Data Model Sketch

Use additive tables or materialized outputs rather than mutating signal tables:

```sql
backtest_runs(
  run_id,
  strategy_id,
  created_at,
  as_of_cutoff,
  disclosure_lag_days,
  price_source,
  benchmark,
  config_json
)

backtest_positions(
  run_id,
  manager_id,
  security_id,
  event_time,
  knowledge_time,
  entry_date,
  exit_date,
  signal_weight,
  position_weight,
  total_return,
  benchmark_return
)

backtest_metrics(
  run_id,
  metric_name,
  metric_value,
  dimension_name,
  dimension_value
)
```

SQLite should store `config_json` as text and date/timestamp values as ISO-8601
strings. Postgres can use `jsonb` and timestamp/date types behind the same
repository helper.

## API And Surface Plan

First implementation:

- command-line runner for fixture-backed strategy replay;
- Python service function returning run, position, and metric objects;
- tests that prove no future holdings are visible before their knowledge time.

Later implementation:

- API endpoint for latest run summaries;
- dashboard table for strategy and manager performance;
- chat context block that can cite a run and explain whether a signal is
  historically supported.

Do not build an operator-facing backtester UI until the internal harness and
price-source decision are stable.

## Acceptance Tests For Implementation

The follow-up code issue should include:

- A bitemporal fixture where a late 13F amendment is invisible before its
  knowledge time and visible after it.
- A strategy fixture that enters after disclosure lag and not on the report
  date.
- A fixture price provider test for absolute, benchmark-relative, and hit-rate
  metrics.
- A missing-price case that records coverage gaps without crashing the run.
- A reproducibility test showing the same strategy config and fixture data
  produce the same `run_id` or state fingerprint.
- A no-UI assertion: the first slice exposes service/CLI output, not a new
  dashboard control.

## Open Risks

- Backtests can overstate edge when price data, delisted securities, and
  corporate actions are incomplete.
- The first fixture provider will prove engine semantics, not production data
  quality.
- Redistribution rules may limit whether benchmark-relative reports can leave
  the local operator environment.
- Activism signals may need campaign lifecycle dates before their backtests are
  comparable to pure holdings-conviction signals.
