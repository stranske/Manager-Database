# Manager-Database

This repository contains the Manager-Database application.

For setup and usage instructions, see `README_bootstrap.md`.

## API Documentation

- [API Rate Limiting](docs/api_rate_limiting.md) - Rate limits, headers, and error handling
- [API Design Guidelines](docs/api_design_guidelines.md) - API design standards and conventions
- [API Changes](docs/api_changes.md) - Historical API modifications
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

**Note:** All API endpoints are subject to rate limiting. See the [API Rate Limiting documentation](docs/api_rate_limiting.md) for details on limits and how to handle rate limit responses.
