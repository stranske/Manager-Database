# API Design Guidelines

## Pagination defaults

List endpoints should return a predictable default page size when clients omit
pagination parameters. The standard default limit is 25 records unless a
specific endpoint documents a different requirement.

## Rate limiting

Rate limiting applies to the chat write paths documented in
[API Rate Limiting](api_rate_limiting.md). Other endpoints are currently
unlimited unless they explicitly delegate to the chat rate limiter in
`api/chat.py`.
