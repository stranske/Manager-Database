# Health Check Failures Runbook

## Purpose
Provide a step-by-step response when the service health checks fail or exceed
latency thresholds.

## Alerts Covered
- `HealthCheckLatencyWarning`: /health p95 latency > 500ms for 5 minutes.
- Any synthetic monitor that reports `/health` returning HTTP 503.

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
