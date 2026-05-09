# API Rate Limiting

This document describes the rate limiting behavior currently shipped by the
Manager-Database API.

## Overview

The API currently applies a small in-process, session-keyed limiter only to
chat write paths that call the chat handler:

- `POST /api/chat`
- `POST /api/chat/filing-summary`
- `POST /api/chat/holdings-analysis`
- `POST /api/chat/query`
- `POST /api/chat/search`
- `POST /api/chat/feedback`

The limiter allows 10 requests per 60 seconds for each session key. The session
key is resolved in this order:

1. `X-Session-Id` request header
2. `session_id` cookie
3. client host

When no session key is available, the fallback key is `unknown`.

## Current Endpoint Scope

| Endpoint | Method | Limit | Key | Response headers |
|----------|--------|-------|-----|------------------|
| `/api/chat` | POST | 10 requests per 60 seconds | session | none |
| `/api/chat/filing-summary` | POST | 10 requests per 60 seconds | session | none |
| `/api/chat/holdings-analysis` | POST | 10 requests per 60 seconds | session | none |
| `/api/chat/query` | POST | 10 requests per 60 seconds | session | none |
| `/api/chat/search` | POST | 10 requests per 60 seconds | session | none |
| `/api/chat/feedback` | POST | 10 requests per 60 seconds | session | none |
| `/chat` | GET | unlimited | n/a | none |
| `/managers` | GET, POST | unlimited | n/a | none |
| `/api/managers/bulk` | POST | unlimited | n/a | none |
| `/api/data` | GET | unlimited | n/a | none |
| `/health`, `/health/*`, `/healthz`, `/livez`, `/readyz` | GET | unlimited | n/a | none |

Other API routes are not rate limited unless they explicitly call the chat
limiter in code.

## 429 Response Shape

When the chat limiter rejects a request, the API returns HTTP 429 with the
standard FastAPI error payload:

```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json

{
  "detail": "Rate limit exceeded"
}
```

The API does not currently emit retry metadata. Clients should not depend on
server-provided retry headers or quota headers.

## Client Guidance

Clients that call `POST /api/chat`, `POST /api/chat/*`, or
`POST /api/chat/feedback` should throttle requests locally by session and use
conservative backoff after a 429 response.
Because the response does not include a reset timestamp, wait up to 60 seconds
before retrying the same session key.

For endpoints outside the chat write paths, this service does not currently
provide server-side quota protection. Clients should still avoid burst traffic
and should use caching or batching where practical.

## Implementation References

- `api/chat.py` defines `InMemoryChatRateLimiter`.
- `api/chat.py` applies that limiter in `POST /api/chat` and the delegated
  `POST /api/chat/*` routes.
- `api/chat.py` applies that limiter in `POST /api/chat/feedback`.
- `tests/test_rate_limit_contract.py` pins this documented behavior.

## Related Documentation

- [API Design Guidelines](api_design_guidelines.md)
- [API Changes](api_changes.md)
- [Health Check Runbook](runbooks/health-checks.md)
