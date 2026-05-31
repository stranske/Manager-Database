.PHONY: db-migrate db-seed app app-smoke

db-migrate:
	python -m alembic upgrade head

db-seed:
	python scripts/seed_managers.py

# Launch the Streamlit analyst UI shell on :8501 for internal/local use.
# Matches the docker-compose `ui` service CMD (ui/Dockerfile). The UI is a
# thin HTTP client of the API on :8000 — point API_BASE_URL/CHAT_API_URL at a
# running `api` (e.g. `docker compose up db api`) to render live data. Auth is
# bypassed locally when UI_USERNAME/UI_PASSWORD are unset (ui/__init__.py).
app:
	streamlit run ui/app.py --server.port=8501 --server.headless=true

# Launch the UI and assert it answers 200 on :8501, then tear it down.
# Exercises scripts/readiness_smoke.py::check_ui against the `make app` path
# without bringing up the full Docker stack.
app-smoke:
	python scripts/readiness_smoke.py --launch-ui
