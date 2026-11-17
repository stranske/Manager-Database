# Manager-Database

This repository contains the Manager-Database application.

For setup and usage instructions, see `README_bootstrap.md`.

The repo also includes a synthetic, offline stlite browser demo for the
deterministic analyst pages. Build it with `python scripts/build_wasm_demo.py`
and serve `web/index.html` locally; details are in `README_bootstrap.md`.

## Local Form 4 ingestion

```text
python -m etl.insider_flow --as-of YYYY-MM-DD [--lookback-days N] [--json]
```

`--as-of` selects which holdings supply issuers. Form 4 fetching uses a rolling
lookback of 90 days by default (`--lookback-days`). Use `--json` to emit the run
summary as JSON; otherwise counters print as `name: value` lines with `null` for
counts the fetcher cannot supply (never `0` as a stand-in for unavailable).

Example (text summary after a successful partial run):

```text
issuers_resolved: 2
filings_scanned: null
rows_inserted: 3
unmatched_issuers: null
skipped_transaction_codes: null
fetch_errors: 1
```

The command uses `connect_db()` for the database connection. Per-issuer fetch
failures increment `fetch_errors` and the run continues. Reruns deduplicate
transactions with a complete key. Deduplication for transactions with missing
key fields is tracked in #1917.

## API Documentation

- [API Rate Limiting](docs/api_rate_limiting.md) - Rate limits, headers, and error handling
- [API Design Guidelines](docs/api_design_guidelines.md) - API design standards and conventions
- [API Changes](docs/api_changes.md) - Historical API modifications
- [Point-In-Time Holdings Design](docs/bitemporal_holdings_design.md) -
  bitemporal holdings migration and as-of query contract
- [Manager Similarity](docs/manager_similarity_design.md) - Peer similarity and crowding design
- [Backtest Signal Alpha Design](docs/backtest_signal_alpha_design.md) -
  point-in-time strategy validation and metrics contract
- [Conviction Cross-Reference](docs/conviction_cross_reference_design.md) -
  Form 4-first conviction context, with short-interest and options staged separately
- [Position Performance Attribution Design](docs/position_performance_attribution_design.md) -
  holdings-to-skill attribution built on the backtest contract
- [Activism Campaign Lifecycle Design](docs/activism_campaign_lifecycle_design.md) -
  campaign entity, timeline, and staged activism scope
- [Memory Profiler](docs/memory_profiler.md) - Background memory leak diagnostics

## API examples

Replace `http://localhost:8000` with your deployed API base URL.

```bash
curl -G "http://localhost:8000/chat" --data-urlencode "q=What is the latest holdings update?"
```

```bash
curl -X POST "http://localhost:8000/managers" \
  -H "Content-Type: application/json" \
  -d '{"name":"Grace Hopper","email":"grace@example.com","department":"Engineering"}'
```

```bash
curl "http://localhost:8000/health/db"
```

**Note:** Chat write endpoints are rate limited. See the [API Rate Limiting documentation](docs/api_rate_limiting.md) for the exact endpoint matrix and response contract.
