# Manager-Intel Bootstrap

This repo provides a minimal stack to begin experimenting with the Manager-Intel project.

## Quick start

1. Copy `.env.example` to `.env` and adjust passwords as needed. The file now
   includes a `DB_URL` pointing at the bundled Postgres container.
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

3. Start the Streamlit dashboard:
   ```bash
   streamlit run ui/dashboard.py
   ```

4. Explore daily reports and news search:
   ```bash
   streamlit run ui/daily_report.py
   ```
   ```bash
   streamlit run ui/search.py
   ```
5. Upload your own notes:
   ```bash
   streamlit run ui/upload.py
   ```

6. Start the chat API:
   ```bash
   uvicorn api.chat:app --reload
   ```

## Manager API validation

The manager create endpoint enforces required fields and basic format rules:

- `POST /managers` with JSON body containing `name`, `email`, and `department`.
- `name` and `department` must be non-empty strings.
- `email` must be a valid email address.
- Validation errors return HTTP 400 with an `errors` list containing `field` and `message`.

## Further reading

- SEC EDGAR API docs[^1]
- Companies House API swagger[^2]
- Prefect 2.0 "flows & deployments" guide[^3]
- Streamlit authentication examples[^4]

[^1]: https://www.sec.gov/os/accessing-edgar-data
[^2]: https://developer.company-information.service.gov.uk/documentation
[^3]: https://docs.prefect.io/latest/guides/flows/#deployments
[^4]: https://github.com/mkhorbani/streamlit-authenticator
