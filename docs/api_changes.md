# API Changes

## 2026-05-09

- **Rate limiting contract**: `docs/api_rate_limiting.md` now reflects the
  shipped API behavior: a session-keyed 10 requests per 60 seconds limiter on
  `POST /api/chat` and `POST /api/chat/feedback`, no global per-IP limiter, and
  no retry or rate-limit response headers.
  - Confirmation: `tests/test_rate_limit_contract.py` pins the documented
    behavior across chat, manager, data, and health endpoints.

## 2026-01-25

- **GET /managers**: The default pagination limit is now always set to 25 when the
  `limit` parameter is omitted.
  - Confirmation: Reviewed against docs/api_design_guidelines.md (Pagination defaults)
    on 2026-01-25; aligns with API design guidelines for list endpoints.
