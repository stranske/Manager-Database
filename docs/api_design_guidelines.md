# API Design Guidelines

## Pagination defaults

List endpoints should return a predictable default page size when clients omit
pagination parameters. The standard default limit is 25 records unless a
specific endpoint documents a different requirement.

## Rate limiting

All API endpoints are subject to rate limiting to ensure fair usage and system
stability. For detailed information about rate limits, handling 429 responses,
and best practices, see [API Rate Limiting](api_rate_limiting.md).
