# UX Review Log — Manager-Database

Diff-anchored record of UX Review (`/ux-review`) passes. Each entry's commit SHA anchors the next
review's git-diff focus. Detailed artifacts live in `Orchestrator/ux_reviews/`.

## 2026-06-22 — Offline stlite build (`web/wasm_app.py`), full coverage — commit `c673705` — overall 6.0/10 (gate FAIL)

- **Scope:** reviewed the OFFLINE browser build (Tier-A PC-deployment path), self-contained on a synthetic SQLite (`scripts/build_wasm_demo.py`). The full client-server app (`ui/app.py` + FastAPI `:8000` + Postgres + MinIO; live data / auth / uploads / Research) was **NOT** reviewed — needs the docker stack (separate pass).
- **Coverage:** Dashboard ✓ (real synthetic data; works); Daily Report ✓ (empty default date); Search ✓ (form renders; query not driven via automation); Upload ✓ (form renders). **NOT driven:** search query submission; full client-server app.
- **Scores:** wired 7.5 / usability 5.5 / help_clarity 4.5 / workflow 6.0 (3 sev-3 findings, all 4/4).
- **Headline:** the offline build is **functional** (Dashboard renders real data) — but it opens on empty default views and leaks a dev auth notice, landing just below the 7.0 gate.
- **Findings → filed:**
  - Daily Report defaults to an empty date + Dashboard "Recent Activity (30 days)" empty (demo data older than 30 days); no jump-to-latest / nearest-date guidance → **#1214**.
  - Dev auth-bypass notice renders into the main UI (`ui/__init__.py:28` `st.warning`) → **#1215**.
- **Next focus:** after #1214/#1215, re-check the offline build; and run a SEPARATE full client-server pass (docker stack) to review live data, auth, uploads, and the Research page.
