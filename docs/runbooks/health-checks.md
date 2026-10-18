# Health Check Failures Runbook

## Purpose
Provide a step-by-step response when the service health checks fail or exceed
latency thresholds.

## Alerts Covered
- `HealthCheckLatencyWarning`: /health p95 latency > 500ms for 5 minutes.
- Any synthetic monitor that reports `/health` returning HTTP 503.
- Data-quality `etl_failure` events built from freshness and row-count checks.

## Quick Triage
1. Confirm alert scope: identify environment, region, and deployment version.
2. Check `/health` response body for `failed_checks` reasons.
3. Verify whether failures are isolated to one dependency or systemic.

## Immediate Actions
- If `failed_checks` includes `database`, verify database connectivity.
- If `failed_checks` includes `minio`, validate object storage availability.
- If `failed_checks` includes `redis`, verify cache connectivity and DNS.
- If `failed_checks` is empty but status is 503, inspect recent deploys.

## Diagnostics

### Data Quality And Freshness

- `alerts.data_quality.evaluate_daily_quality()` defines the standing checks for
  harvest freshness, news max age, row-count drops, and unmapped-CUSIP rate.
- Configure thresholds with `DQ_HARVEST_WINDOW_MINUTES`,
  `DQ_NEWS_MAX_AGE_HOURS`, `DQ_ROW_COUNT_DROP_PCT`, and
  `DQ_UNMAPPED_CUSIP_RATE_PCT`.
- `etl.data_quality_flow.data_quality_flow` gathers persisted inputs and calls
  these checks. Its `data_quality_deployment` is scheduled at 08:30 in the same
  local timezone as daily-diff. Serve/register this deployment with the existing
  Prefect worker; importing the module defines it but does not start a scheduler.
  A local one-off invocation is `python -m etl.data_quality_flow`.
- Landing evidence is `MAX(created_at)` from EDGAR/SEC filings and news rows,
  with their persisted row counts; it does not prove an empty harvest executed.
  News age uses `MAX(published_at)`. Table-count baselines use the latest earlier
  `data_quality_runs` snapshot; the first date has no drop baseline. Unmapped
  CUSIPs are distinct current holdings without a ticker/FIGI on the holding or
  in the identifier cache. No provider lookup is performed by this flow.
- The flow commits an immutable UTC-date snapshot in `data_quality_runs` before
  calling `alerts.integration.fire_data_quality_alerts()`, which builds an
  `etl_failure` event and calls the ordinary dispatcher/configured channels.
  Same-day reruns reuse that snapshot and UTC-midnight event timestamp, so the
  existing outbox claims prevent duplicate sends even as source data changes.
  Run the following day for a new snapshot. Rules/channels must be configured;
  an unmatched rule or unavailable channel is not evidence of external delivery.
- `evaluate_and_record_data_quality_alerts()` remains a record-only helper;
  it does not dispatch notifications. Check failures are warnings, and delivery
  exceptions return warnings without failing the ingestion flows. Reconcile
  unknown outbox claims before a manual retry; claims are never auto-released.
- Verify offline delivery, replay and schedule wiring with
  `python -m pytest tests/test_data_quality.py -k data_quality_flow -q`.

### Database
- Confirm DB endpoint reachability (security groups, network ACLs).
- Validate connection limits and lock contention.
- Check DB logs for timeouts or refused connections.

### MinIO / Object Storage
- Confirm endpoint and credentials in environment variables.
- Check MinIO service health and bucket listing permissions.
- Inspect network latency between app and storage.

### Redis
- Confirm `REDIS_URL` configuration is correct.
- Validate Redis service is up and accepting connections.
- Review slowlog for long-running commands.

## Latency-Specific Steps
- For `HealthCheckLatencyWarning`, compare `/health` latency with dependency
  metrics (database ping time, MinIO request duration, Redis ping time).
- If only one dependency is slow, isolate and throttle retries if needed.
- If all dependencies are slow, inspect node CPU, memory, and I/O saturation.

## Remediation
- Restart unhealthy dependency services if safe to do so.
- Roll back recent deploys if health degraded after release.
- Adjust dependency timeouts only after confirming network stability.

## Validation
- Re-run `/health` and verify HTTP 200 with empty `failed_checks`.
- Confirm alert clears after two consecutive healthy intervals.

## Follow-Up
- Record incident timeline and root cause in the incident log.
- Add or update monitors if a dependency was missing alert coverage.

<!-- Commit-message checklist:
- [ ] type is accurate (feat, fix, test)
- [ ] scope is clear (health)
- [ ] summary is concise and imperative
-->
