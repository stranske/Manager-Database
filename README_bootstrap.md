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

### Offline browser demo

The deterministic analyst pages can also be bundled for a zero-install,
zero-egress browser demo that uses only synthetic fixture data and excludes the
Research/LLM boundary.

```bash
python scripts/build_wasm_demo.py
python -m http.server 8000 -d web
```

Then open `http://localhost:8000/index.html`. The page loads stlite/Pyodide in
the browser, sets `UI_OFFLINE=1`, leaves `DB_URL` unset, points `DB_PATH` at the
bundled `manager_demo.sqlite`, and renders Dashboard, Daily Report, Search, and
Upload against seeded synthetic rows. The Upload page uses `USE_SIMPLE_EMBED=1`
so no model download is attempted. Do not use this bundle for proprietary data
on a public host; rebuild it only from synthetic or explicitly redacted inputs.

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

3. Generate the local analyst digest without sending email:
   ```bash
   DIGEST_DRY_RUN=true DIGEST_OUTPUT_PATH=/tmp/manager-digest.txt python etl/digest_flow.py
   ```
   The digest reads recent `filings`, manager-linked `news_items`, and unacknowledged
   important `alert_history` rows from `DB_PATH`/`DB_URL`. Configure
   `DIGEST_LOOKBACK_HOURS`, `DIGEST_EMAIL_TO`, and `DIGEST_EMAIL_FROM` for scheduled
   delivery; it reuses the existing SMTP/SendGrid environment variables from alert email
   delivery.

4. Start the Streamlit app shell:
   ```bash
   streamlit run ui/app.py
   ```

   ### Run the UI locally (no full Docker stack)

   For internal/local browsing you don't need to hand-orchestrate Postgres +
   MinIO + uvicorn. A single command launches the multipage analyst shell on
   `:8501` (the exact command the docker-compose `ui` service uses):

   ```bash
   make app
   # or, after `pip install -e .`:
   mgrdb-app
   ```

   This stays inside the org perimeter — the UI runs as a local/internal
   process; **do not** publish it to Streamlit Community Cloud with real
   manager data.

   - **API target.** The UI is a thin HTTP client of the API on `:8000`; it
     cannot render live data without a reachable API. `ui/alerts.py` reads the
     API root from `API_BASE_URL` (default `http://localhost:8000`), while
     `ui/research.py` posts directly to `CHAT_API_URL` (default
     `http://localhost:8000/api/chat`). The lightest internal path is to bring
     up just the `api` + `db` compose services, then run `make app`:
     ```bash
     docker compose up -d db api
     API_BASE_URL=http://localhost:8000 CHAT_API_URL=http://localhost:8000/api/chat make app
     ```
     Point `API_BASE_URL` at any already-running API root and `CHAT_API_URL` at
     that API's chat endpoint for UI-only mode.
   - **Auth is bypassed locally.** When `UI_USERNAME`/`UI_PASSWORD` are unset,
     `ui/__init__.py` skips `streamlit_authenticator` (logging a dev-mode
     warning) and treats the session as authenticated. Leave both unset for
     local/internal mode; keep them **set** in production.
   - **Smoke-test the launch path.** `make app-smoke` (i.e.
     `python scripts/readiness_smoke.py --launch-ui`) launches the UI via the
     same command and asserts it answers `200` on `:8501`, then tears it down —
     without bringing up the full stack.

5. You can still run individual pages directly if needed:
   ```bash
   streamlit run ui/daily_report.py
   ```
   ```bash
   streamlit run ui/search.py
   ```
   The multipage shell exposes routes like `/daily-report`, `/search`, `/upload`, and `/research`.
6. Upload your own notes:
   ```bash
   streamlit run ui/upload.py
   ```

7. The chat API runs as the `api` compose service started in step 2 of the
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

### EDGAR Postgres persistence smoke

The EDGAR ingest path has a strict Postgres-style persistence smoke that
rejects SQLite-only SQL while verifying raw upload, document metadata,
canonical filing and holding writes, and `new_filing` alert dispatch:

```bash
pytest tests/test_edgar_flow.py::test_fetch_and_store_uses_postgres_safe_persistence -q
```

Run it with the schema bootstrap smoke above when changing compose `DB_URL`,
MinIO, or canonical filing/holding columns.

The Schema Idempotence workflow also runs
`scripts/verify_schema_idempotence.sh` against a `pgvector/pgvector:pg16`
service on schema-related pull requests, pushes to `main`, nightly schedule,
and manual dispatch. That gate applies `schema.sql` twice to a fresh Postgres
database, so non-idempotent schema changes fail in CI before merge.

## Further reading

- SEC EDGAR API docs[^1]
- Companies House API swagger[^2]
- Prefect 2.0 "flows & deployments" guide[^3]
- Streamlit authentication examples[^4]

[^1]: https://www.sec.gov/os/accessing-edgar-data
[^2]: https://developer.company-information.service.gov.uk/documentation
[^3]: https://docs.prefect.io/latest/guides/flows/#deployments
[^4]: https://github.com/mkhorbani/streamlit-authenticator
