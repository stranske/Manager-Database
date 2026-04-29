# Manager-Intel Bootstrap

This repo provides a minimal stack to begin experimenting with the Manager-Intel project.

## Quick start

1. Copy `.env.example` to `.env` and adjust passwords as needed. The file now
   includes a `DB_URL` pointing at the bundled Postgres container. Set
   `UI_USERNAME` and `UI_PASSWORD` to enable Streamlit UI authentication (if
   unset, the UI runs without authentication for local development).
2. Start the local product stack:
   ```bash
   docker compose up -d db minio api ui
   ```
   This brings up Postgres, MinIO, the FastAPI service (`api.chat:app` on
   port 8000), and the Streamlit UI (port 8501) in one step. The placeholder
   ETL container can be started alongside with `docker compose up -d etl` if
   you also want to exercise the ingest path.
3. Seed the baseline manager records and run the readiness smoke:
   ```bash
   python scripts/seed_managers.py
   python scripts/readiness_smoke.py
   ```
   The smoke verifies `/health/detailed`, `/managers`, and `/chat` against
   the locally seeded data and exits non-zero if any dependency is
   unreachable. Pass `--base-url` to point it at a non-default API URL.
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

## Further reading

- SEC EDGAR API docs[^1]
- Companies House API swagger[^2]
- Prefect 2.0 "flows & deployments" guide[^3]
- Streamlit authentication examples[^4]

[^1]: https://www.sec.gov/os/accessing-edgar-data
[^2]: https://developer.company-information.service.gov.uk/documentation
[^3]: https://docs.prefect.io/latest/guides/flows/#deployments
[^4]: https://github.com/mkhorbani/streamlit-authenticator
