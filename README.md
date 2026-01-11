# Manager-Database

This repository contains the Manager-Database application.

For setup and usage instructions, see `README_bootstrap.md`.

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
