# 🤖 Agents.md  
*A playbook for GitHub Copilot‑Chat / Code‑Assist (“Codex”) on this repo*  

> **Why this file?**  
> 1. The project has many moving parts (ETL, storage, UI, infra).  
> 2. You, dear human, said you’re new to scraping & databases.  
> 3. Codex needs explicit, staged instructions so it knows when to charge ahead and when to raise its digital hand.

---

## 0. Ground rules for Codex

| Rule # | Guideline |
|-------|-----------|
| **R1** | **Stay inside the stage.** Work only on tasks defined for the current stage unless the user explicitly says otherwise. |
| **R2** | **Write, then explain.** For every code change you propose, add inline comments *and* a commit‑message checklist. |
| **R3** | **One PR per atomic feature.** If a change affects >1 module, create separate branches / PRs. |
| **R4** | **When stuck, escalate.** Follow §7 *When to Ask for Outside Help*. |
| **R5** | **Keep GitHub tidy.** Always open/close Issues and update the Project board (§6) as you work. |

---

## 1. Repository map (auto‑generated cheat‑sheet)
.
├── adapters/            # Data source modules (edgar, uk, canada, base)
├── api/                 # FastAPI endpoints (chat)
├── etl/                 # Prefect flows & helpers
│   ├── edgar_flow.py
│   ├── daily_diff_flow.py
│   └── summariser_flow.py
├── ui/                  # Streamlit pages
│   ├── dashboard.py
│   ├── daily_report.py
│   ├── search.py
│   └── upload.py
├── tests/               # Pytest suite
├── embeddings.py        # Embedding utilities
├── diff_holdings.py
├── docker-compose.yml
├── schema.sql
├── README_bootstrap.md
├── .pre-commit-config.yaml
└── (this) Agents.md

*(update with `tree -L 2` occasionally)*

---

## 2. Prerequisites Codex assumes

1. **Python 3.12** available via `pyenv`.  
2. **Docker & Compose v2** installed.  
3. You have run `docker compose up -d` successfully at least once.  
4. Environment variables live in `.env` (never commit secrets).

---

## 3. Stage overview  
*(mirrors the phased roadmap in* `Manager‑Intel‑Platform.md`*)*

| Stage | Goal | Human inputs Codex needs |
|-------|------|--------------------------|
| **S0 – Bootstrap** | Local stack running; cost‑log table created | A working Docker Desktop install |
| **S1 – Proof‑of‑Concept** | EDGAR adapter + holdings diff in **SQLite** | **CIK_LIST="0001791786,0001434997"** |
| **S2 – Production ETL** | Prefect + Postgres + MinIO; multi‑country adapters; tracked_call wrapper | API keys (SEC, Companies House, etc.) |
| **S3 – Analyst Portal** | Streamlit dashboard skeleton | Basic brand colour / logo (optional) |
| **S4 – AI‑Assist (opt.)** | Vector embeddings + RAG chat | OpenAI key & model preferences |

Codex must not advance past a stage until all exit‑criteria in §4 are met **and** an Issue titled `Promote to next stage` is closed.

---

## 4. Detailed task list per stage

### 4.1 Stage 0 — Bootstrap

1. **Generate** `docker-compose.yml` exactly as specified in *Manager‑Intel‑Platform.md*, but parameterise passwords via `.env`.  
2. **Create** `schema.sql` containing `api_usage` table.  
3. **Write** a GitHub Action:  
   * On push to `main` → run `docker compose up -d && pytest -q`.  
4. **Open Issues automatically**:  
   * `#doc Improve README_bootstrap.md` (placeholder exists but has one line)
   * `#infra Add GitHub Action for CI`

**Exit‑criteria**: `docker compose up` prints no errors; `api_usage` exists; CI badge shows green.

---

### 4.2 Stage 1 — Proof‑of‑Concept

1. **Implement** `adapters/edgar.py` with three async coroutines:  
   ```python
   async def list_new_filings(since): ...
   async def download(filing): ...
   async def parse(raw): ...

2. Write a Prefect flow in etl/edgar_flow.py that:

Accepts CIK_LIST via env‑var (defaults to Elliott & SIR).

Saves raw JSON in minio://filings/raw/.

Inserts parsed rows into SQLite (dev.db).

3. Add a tiny diff_holdings.py script:

python diff_holdings.py 0000320193 → prints additions / exits.

4. Unit tests (Pytest) covering:

Happy‑path parse of Apple’s last 13F.

Fails gracefully on HTTP 429.

5. **Sanity check**: raise `UserWarning` if a manager’s `filings.recent` array
   doesn't include any `13F-HR`—flagging the edge‑case where a future test CIK
   isn’t an institutional filer.

Exit‑criteria: diff_holdings.py works for both sample CIKs; tests pass.

4.3 Stage 2 — Production ETL
Migrate from SQLite to Postgres container.

Introduce tracked_call() wrapper around every HTTP request (log to api_usage).

Add UK & Canada adapters; common base class lives in adapters/base.py.

Refactor Prefect to parameterise jurisdiction.

Upgrade tests: nightly regression via pytest -m nightly.

Security:

Encrypt MinIO bucket (sse-s3).

Store AWS creds in GitHub Secrets.

Exit‑criteria: 90% test coverage; monthly_usage materialised view returns rows.

### 4.3.1 ➡️ **New sub‑stage 3.1 – Daily report GUI**
1. In Streamlit, add `/daily_report.py`
   * Layout: date‑picker (defaults to *yesterday*), two tabs
     * **Filings & Diffs** – table of filings + coloured Δ arrows
     * **News Pulse** – top 20 NLP‑tagged headlines
   * Provide “Download CSV” button.
2. Prefect cron: regenerate yesterday’s diff by 08:00 local time; cache in
   Postgres for fast page loads.

**Exit‑criteria:** page renders in <500 ms on laptop; csv matches on‑screen grid.

4.4 Stage 3 — Analyst Portal
Generate a Streamlit app skeleton (ui/).

Pages:

dashboard.py – delta table + sparkline.

search.py – FTS query box.

Auth: simple cookie‑based stub (st_authenticator).

Docker‑compose override: expose Streamlit on 8501.

Exit‑criteria: User can log in and see holdings diff & latest news.

4.5 Stage 4 — AI‑Assist (optional)
Embed each new document with sentence-transformers/all-MiniLM-L6-v2, store in PGvector.

Add /chat endpoint (FastAPI) for RAG.

Summariser: Prefect task that posts a markdown summary to an internal Slack/webhook.

Exit‑criteria: Chat endpoint returns coherent replies.

5. Coding patterns Codex must follow
Branch naming: stage{N}/feature‑short‑desc (e.g. stage2/adapter‑uk).

Commits: Conventional Commits (feat:, fix:, chore:…).

Testing: Pytest; run docker compose exec etl pytest -q.

Linting: Ruff + Black; include pre-commit config.

6. GitHub hygiene workflow (automated by Codex)
Issues:

For every task bullet in §4, open a matching Issue if none exists.

Close Issues automatically via PR description (Closes #12).

Project board (Projects ∞):

Columns: Backlog → In Progress → Review → Done.


Move the card whenever pushing to a branch tagged stage*.

Milestones: Mirror the M1…M6 table in Manager‑Intel‑Platform.md and keep due‑dates in sync (fetch via raw.githubusercontent.com).

Codex must run a “board‑sync” routine at the end of each session:
gh project item-status --project "Manager‑Intel" \
  --item "$ISSUE_ID" --status "In Progress"

7. When Codex should escalate / ask the human
| Situation                                 | Escalation text template                                               |
| ----------------------------------------- | ---------------------------------------------------------------------- |
| Missing credentials / API key             | “Please add **XYZ\_API\_KEY** to repository secrets and ping me.”      |
| Ambiguous business rule (e.g. diff logic) | “I found two equally valid interpretations of … Which one?”            |
| External service down >2 h                | “Endpoint *foo* has been 5xx since <timestamp>. Proceed to mock? Y/N.” |
| Unit‑test design unclear                  | “Provide an example input/output pair for …”                           |


8. Quick‑reference commands for the human
# bring everything up
docker compose up -d

# run Prefect flow locally
prefect flow-run run --name edgar-test

# open psql shell
docker exec -it db psql -U postgres


9. Further reading links
SEC EDGAR API docs

Companies House API swagger

Prefect 2.0 “flows & deployments” guide

Streamlit authentication examples

(Codex: turn each into footnote‑style links in README when relevant.)

---

### 📋 How Codex will keep GitHub Issues & Projects up‑to‑date
Yes—having Codex draft Issues/Project‑board cards is both **possible** and **strongly advisable**.  
The instructions in §6 tell it to:

1. Parse every unchecked box in §4 into a new Issue (if one doesn’t exist).  
2. Auto‑move the Issue’s card across *Backlog → In Progress → Done* as soon as it pushes code or opens/merges a PR.  
3. Regenerate milestone due‑dates from the roadmap table so “real” and “source‑of‑truth” never drift.

Tools Codex can call:

```bash
# create issue
gh issue create --title "Stage1: Edgar adapter" --body "...details..."

# add to project
gh project item-add --project "Manager‑Intel" --issue <id>
These commands already work with GitHub CLI; VS Code’s built‑in Copilot Chat has permission to invoke them if you’re signed‑in.

1️⃣ Sample managers (CIKs locked‑in)

| Manager | Description | Confirmed CIK | Sources |
| ------- | ----------- | ------------- | ------- |
| Elliott Investment Management L.P. | Global multi‑strategy activist fund | 0001791786 | [sec.gov](https://www.sec.gov), [13f.info](https://13f.info) |
| SIR Capital Management L.P.<br/><small>umbrella for “Standard Investment Research” energy vehicles</small> | Oil, gas & new‑energy hedge fund | 0001434997 (13F File No. 028‑13426) | [research.secdatabase.com](https://research.secdatabase.com), [sec.gov](https://www.sec.gov) |

Small print: SIR files genuine 13Fs under the management entity above; their various fund SPVs only file Form D. Using the manager‑level CIK lets the 13F POC work without surprises.

<!-----------------  🔧  CONFIG / METRIC UPGRADE  ---------------->
### :construction_worker: Task — ensure ShortfallProb is always produced

1 · **parameters template**  
   * open `config/parameters_template.csv`  
   * append a line (or update if it exists)  
     ```
     risk_metrics,Return,Risk,ShortfallProb
     ```
   * propagate the same change to any YAML sample (`params_template.yml`).

2 · **CLI sanity-check** (`pa_core/config.py`)  
   * on load, assert `"ShortfallProb" in cfg.risk_metrics`;  
     if absent, raise `ConfigError("risk_metrics must include ShortfallProb")`.

3 · **Excel exporter** (`pa_core/reporting/excel.py`)  
   * before writing the *Summary* sheet:  
     ```python
     summary["ShortfallProb"] = summary.get("ShortfallProb", 0.0)
     ```
     so old output files never explode the viz.

4 · **Dashboard guard-rail** (`pa_core/viz/risk_return.py`)  
   * same one-liner:  
     ```python
     df = df.copy()
     df["ShortfallProb"] = df.get("ShortfallProb", 0.0)
     ```

5 · **Regression test** (`tests/test_outputs.py`)  
   ```python
   import pandas as pd, pathlib
   def test_shortfall_present():
       fn = pathlib.Path("Outputs.xlsx")
       assert fn.exists(), "Outputs.xlsx missing"
       cols = pd.read_excel(fn, sheet_name="Summary").columns
       assert "ShortfallProb" in cols

