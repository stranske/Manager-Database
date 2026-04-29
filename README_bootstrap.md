# Manager-Intel Bootstrap

This repo provides a minimal stack to begin experimenting with the Manager-Intel project.

## Quick start

1. Copy `.env.example` to `.env` and adjust passwords as needed. The file now
   includes a `DB_URL` pointing at the bundled Postgres container. Set
   `UI_USERNAME` and `UI_PASSWORD` to enable Streamlit UI authentication (if
   unset, the UI runs without authentication for local development).
2. Run `docker compose up -d` to start Postgres, MinIO and a placeholder ETL container.
3. Run `pytest -q` to verify the environment.

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

6. Start the chat API:
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
Alembic tests cannot see), run:

```bash
docker compose up -d db
export MGRDB_PG_TEST_URL=postgresql://postgres:$POSTGRES_PASSWORD@localhost:5432/postgres
pytest tests/test_schema_postgres_bootstrap.py
```

The smoke drops and recreates the `public` schema before each test, applies
`schema.sql`, and asserts that the API/ETL-critical tables, materialized
views, and indexes (notably `mv_daily_report` and `mv_daily_report_idx`) are
all present. Tests skip cleanly when `MGRDB_PG_TEST_URL` is unset, so the
SQLite-only CI path is unaffected.

## Further reading

- SEC EDGAR API docs[^1]
- Companies House API swagger[^2]
- Prefect 2.0 "flows & deployments" guide[^3]
- Streamlit authentication examples[^4]

[^1]: https://www.sec.gov/os/accessing-edgar-data
[^2]: https://developer.company-information.service.gov.uk/documentation
[^3]: https://docs.prefect.io/latest/guides/flows/#deployments
[^4]: https://github.com/mkhorbani/streamlit-authenticator
