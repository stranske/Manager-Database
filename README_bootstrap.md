# Manager-Intel Bootstrap

This repo provides a minimal stack to begin experimenting with the Manager-Intel project.

## Quick start

1. Copy `.env.example` to `.env` and adjust passwords as needed. The file now
   includes a `DB_URL` pointing at the bundled Postgres container. Set
   `UI_USERNAME` and `UI_PASSWORD` to enable Streamlit UI authentication (if
   unset, the UI runs without authentication for local development).
2. Run the one-command local readiness smoke:
   ```bash
   python scripts/readiness_smoke.py
   ```
   The smoke resets compose state, starts Postgres, MinIO, the FastAPI service
   (`api.chat:app` on port 8000), and the Streamlit UI (port 8501), seeds the
   deterministic manager/research fixtures inside the API container, probes
   `/health/detailed`, `/managers`, `/chat`, and verifies the UI is reachable.
   It exits non-zero if the API, database, object storage, manager route,
   chat/research route, or UI is not ready. Pass `--base-url` or `--ui-url` to
   target non-default ports. Use `--skip-stack-start` only when you have already
   started the stack and want to seed through the local Python environment.
3. If you want to bring up services manually for iterative work:
   ```bash
   docker compose up -d db minio api ui
   ```
   The placeholder ETL container can be started alongside with
   `docker compose up -d etl` if you also want to exercise the ingest path.
4. Run `pytest -q` to verify the rest of the test suite.

The `schema.sql` file defines an `api_usage` table used for cost telemetry. Apply it to the Postgres container once it is running:

```bash
docker exec -i <db_container_name> psql -U postgres -f /path/to/schema.sql
```

Feel free to open issues or pull requests as you iterate.

## Running the ETL flow

1. Ensure Python 3.12 is available via `pyenv` and install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Trigger the sample EDGAR flow:
   ```bash
   python etl/edgar_flow.py
   ```
   Parsed rows will be stored in `dev.db` and raw filings uploaded to the `filings` bucket in MinIO.

3. Start the Streamlit app shell:
   ```bash
   streamlit run ui/app.py
   ```

4. You can still run individual pages directly if needed:
   ```bash
   streamlit run ui/daily_report.py
   ```
   ```bash
   streamlit run ui/search.py
   ```
   The multipage shell exposes routes like `/daily-report`, `/search`, `/upload`, and `/research`.
5. Upload your own notes:
   ```bash
   streamlit run ui/upload.py
   ```

6. The chat API runs as the `api` compose service started in step 2 of the
   Quick start. To run it directly against an out-of-compose environment
   (for example with `--reload` for fast iteration), you can still launch
   it manually:
   ```bash
   uvicorn api.chat:app --reload
   ```

### API examples

Chat query:

```bash
curl "http://localhost:8000/chat?q=What%20is%20the%20latest%20holdings%20update%3F"
```

Database health check:

```bash
curl http://localhost:8000/health/db
```

### Manager API validation

The API expects manager records to include non-empty `name`, valid `email`, and
non-empty `department` values.

Example request:

```bash
curl -X POST http://localhost:8000/managers \
  -H "Content-Type: application/json" \
  -d '{"name":"Grace Hopper","email":"grace@example.com","department":"Eng"}'
```

Validation failures return HTTP 400 with field-level error messages:

```json
{
  "errors": [
    {"field": "email", "message": "value is not a valid email address"}
  ]
}
```

## Schema bootstrap smoke

`schema.sql` is mounted into Postgres at compose-up via
`/docker-entrypoint-initdb.d/schema.sql`. To verify it bootstraps a clean
database end-to-end (catching object-ordering bugs that the SQLite-side
Alembic tests cannot see), run these commands from a fresh checkout:

```bash
# 1. Create your local environment file (only needed once)
cp .env.example .env

# 2. Start the Postgres container (schema.sql is applied automatically on first start)
docker compose up -d db

# 3. Point the smoke test at the local container.
#    The default password from .env.example is "postgres"; adjust if you changed it.
export MGRDB_PG_TEST_URL=postgresql://postgres:postgres@localhost:5432/postgres

# 4. Run the bootstrap smoke
pytest tests/test_schema_postgres_bootstrap.py
```

If you customised `POSTGRES_PASSWORD` in your `.env`, substitute it in the URL:

```bash
export MGRDB_PG_TEST_URL=postgresql://postgres:<your-password>@localhost:5432/postgres
```

The smoke drops and recreates the `public` schema before each test, applies
`schema.sql` statement-by-statement, and asserts that the API/ETL-critical
tables, materialized views, and indexes (notably `mv_daily_report` and
`mv_daily_report_idx`) are all present. If any statement fails the test
reports the exact statement and error. Tests skip cleanly when
`MGRDB_PG_TEST_URL` is unset, so the SQLite-only CI path is unaffected.

## Further reading

- SEC EDGAR API docs[^1]
- Companies House API swagger[^2]
- Prefect 2.0 "flows & deployments" guide[^3]
- Streamlit authentication examples[^4]

[^1]: https://www.sec.gov/os/accessing-edgar-data
[^2]: https://developer.company-information.service.gov.uk/documentation
[^3]: https://docs.prefect.io/latest/guides/flows/#deployments
[^4]: https://github.com/mkhorbani/streamlit-authenticator
