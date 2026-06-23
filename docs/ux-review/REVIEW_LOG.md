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

## 2026-06-23 — Re-test after #1214 + #1228 — commit `47bf0ed` — overall 3.0/10 (gate FAIL — REGRESSION from 6.0)

- **Coverage:** shipped offline stlite build (`web/index.html`, built via `scripts/build_wasm_demo.py`) ✓ DRIVEN in headless Chromium served locally — **FAILS to boot**; server-side render (`wasm_app.py`) ✓ (Dashboard renders real data); Daily Report function/code-verified (#1214 fix). **Not driven:** Search, Upload, Manager-selector (→ next focus).
- **Scores:** wired 2.5 / usability 2.5 / help_clarity 4.0 / workflow 3.0; one sev-4 blocker; adversarial critic refuted nothing. Panel: claude 3/3/4/3 · codex 2/2/4/2 · cursor 3/4/5/3 · vibe 2/2/4/3.
- **Headline (REGRESSION 6.0 → 3.0):** the offline build that booted at the prior review is now broken. `#1228` ("Vendor offline stlite runtime") removed the CDN dependency (good — fixes the #1220 CDN half) but introduced a boot break: the relative `pyodideUrl` (`web/index.html:44`) resolves against the stlite build's assets base → `…/build/assets/vendor/pyodide/v0.27.3/full/pyodide.mjs` 404 → "Error during booting up". Same root-cause family as IMI #639. The `wheelUrls` (`:46-47`) are relative too (boot stalls even after patching pyodideUrl absolute). App logic is healthy server-side — purely offline packaging.
- **Findings → disposition:** #1214 (empty views) **FIXED** (#1223: Daily Report defaults to `latest_report_date()` + "Go to latest report date" button + "nearest report is <date>" message). #1220 (offline boot, deployment-hardening; #1228 was an incomplete attempt, correctly reopened) — re-test evidence + the pyodideUrl/wheelUrls fix added as a comment. #1215 (dev auth-bypass notice `ui/__init__.py:28`) **still present** — panel-corroborated; commented. No new issues.
- **Next focus:** after #1220 lands, re-drive `web/index.html` offline (Dashboard must render, zero CDN); add a graceful boot-failure fallback (cf. Pension-Data); drive Search/Upload/Manager-selector; the docker full client-server pass still pending.
