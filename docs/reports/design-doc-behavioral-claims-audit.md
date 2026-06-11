# Design Doc Behavioral Claims Audit

Issue: #1149

Generated: 2026-06-11

## Summary

This audit enumerates behavioral claims from operator-facing design/spec
documents and checks them against the current implementation and tests.

| Classification | Count |
| --- | ---: |
| implemented-and-verified | 6 |
| unimplemented | 4 |
| contradicted | 1 |

## Audit Commands

```bash
rg -n -i "nightly|hourly|rate.?limit|all endpoint|scheduled|every day|quota|SLA" Manager-Intel-Platform.md docs README_bootstrap.md
rg -n "edgar_deployment|news_deployment|digest_deployment|daily_diff_deployment|evaluation_flow_deployment|rate_limit|api_design_guidelines|nightly" tests etl .github/workflows README_bootstrap.md docs/api_design_guidelines.md docs/api_rate_limiting.md
```

## Findings

| Doc reference | Claim | Classification | Implementation or gap evidence | Linked issue |
| --- | --- | --- | --- | --- |
| `Manager-Intel-Platform.md:7` | Nightly ETL job hits official APIs, downloads new filings, parses metadata, extracts tables, and stores PDF/text. | unimplemented | Original review drift required by #1149: the claim was not implemented when the audit issue was filed. Current branch evidence (`etl/edgar_flow.py` `edgar_deployment`, `README_bootstrap.md`, and `tests/test_edgar_flow.py::test_edgar_deployment_has_daily_schedule`) shows the scheduling portion was subsequently resolved by #1142, but the audit keeps the required drift classification and link. | Resolved by #1142. |
| `Manager-Intel-Platform.md:9` | Low-latency news harvester hits feeds hourly and tags items with manager/topic metadata. | implemented-and-verified | `etl/news_flow.py` defines `news_deployment` named `news-hourly` with cron `0 * * * *`; `adapters/news.py` supports RSS/GDELT fetching and topic tagging; `tests/test_news_flow.py::test_news_deployment_has_hourly_schedule` pins the schedule. | None. |
| `Manager-Intel-Platform.md:98` | EDGAR access should respect fair-use by throttling requests. | unimplemented | `tests/test_edgar_integration.py::test_rate_limit_handling` covers 429 retry/backoff handling after the remote service responds, and `adapters/news.py` rate-limits GDELT requests, but this audit found no steady-state EDGAR outbound request-rate governor. | Future follow-up if EDGAR live volume grows. |
| `Manager-Intel-Platform.md:110` | Parser breakage is mitigated by nightly parser tests and retained prior HTML snapshots. | unimplemented | `.github/workflows/nightly.yml` runs `pytest -m nightly -q`, but the audit found no retained prior HTML snapshot corpus or nightly parser-regression path for filing adapters. | #1151 |
| `Manager-Intel-Platform.md:112` | Accidental database drops are mitigated by automated nightly S3 snapshots and terraform reprovisioning. | unimplemented | No backup workflow, restore script, terraform module, or restore smoke was found under `.github/workflows`, `scripts`, or `docs`. | #1150 |
| `Manager-Intel-Platform.md:203` | The digest covers the last 24 hours of filings and news. | implemented-and-verified | `etl/digest_flow.py` builds filings, news, and alert digests from a configurable lookback; `README_bootstrap.md` documents `DIGEST_LOOKBACK_HOURS`; `tests/test_digest_flow.py::test_build_digest_collects_recent_filings_news_and_alerts` verifies recent filings, news, and alerts are collected. | None. |
| `README_bootstrap.md:69` | Operators can serve the scheduled EDGAR deployment for the nightly Prefect path. | implemented-and-verified | The documented `prefect deployment serve etl/edgar_flow.py:edgar_deployment` command matches `etl/edgar_flow.py` and is pinned by `tests/test_edgar_flow.py::test_edgar_deployment_has_daily_schedule`. | Resolved by #1142. |
| `README_bootstrap.md:82` | Digest scheduling is controlled by digest environment variables. | implemented-and-verified | `etl/digest_flow.py` reads `DIGEST_LOOKBACK_HOURS`, `DIGEST_DRY_RUN`, `DIGEST_EMAIL_TO`, and `DIGEST_EMAIL_FROM`; `digest_deployment` is `manager-digest-daily` at cron `0 13 * * *`. | None. |
| `README_bootstrap.md:241` | Schema validation runs on schema PRs, pushes to main, nightly schedule, and manual dispatch. | implemented-and-verified | `.github/workflows/schema-idempotence.yml` triggers on `schema.sql`, `scripts/verify_schema_idempotence.sh`, workflow changes, `main`, `schedule`, and `workflow_dispatch`, then runs `bash scripts/verify_schema_idempotence.sh`. | None. |
| `docs/api_design_guidelines.md:11` | Original drift: the guideline claimed all API endpoints were rate limited even though only chat write paths used the limiter. | contradicted | Original review drift required by #1149: `api/chat.py` scoped the limiter to chat write paths while the guideline claimed global API rate limiting. Current branch evidence (`docs/api_rate_limiting.md`, `api/chat.py`, and `tests/test_rate_limit_contract.py`) shows the docs were subsequently aligned by #1145, but the audit keeps the required drift classification and link. | Resolved by #1145. |
| `docs/api_rate_limiting.md:43` | Other API routes are not rate limited unless they explicitly call the chat limiter. | implemented-and-verified | `tests/test_rate_limit_contract.py::test_documented_endpoints_do_not_emit_rate_limit_headers` covers `/chat`, `/managers`, `/api/managers/bulk`, `/api/data`, and health routes as non-429/no rate-limit headers. | Resolved by #1145. |

## Follow-Up Issues Filed

- #1150: Add nightly database snapshot and restore-provisioning contract.
- #1151: Add nightly parser-regression snapshot coverage for filing adapters.

## Notes

The two originally known drift examples from the review packet are classified
as #1149 requires: the EDGAR nightly deployment claim is `unimplemented`, and
the former global rate-limit guideline is `contradicted`. Both rows also record
the current branch evidence showing the per-instance fixes landed in #1142 and
#1145, respectively.
