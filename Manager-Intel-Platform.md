## 📊 Manager-Intel Platform

A lightweight, extensible system to crawl public filings (13Fs, annual reports, overseas registries), parse and diff holdings, harvest news, and serve it all via a searchable UI. Built solo in Docker today, enterprise-ready tomorrow.

| Objective                                                  | Data needed                                                      | Automated workflow                                                                                               |
| ---------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Track every public filing (e.g., 13F, 13D, annual reports) | SEC EDGAR, foreign registries (Companies House UK, MAS SG, etc.) | Nightly ETL job hits the official APIs, downloads new filings, parses metadata, extracts tables, stores PDF/text |
| Know *what changed* quarter-to-quarter                     | Parsed holdings tables, footnote deltas                          | Post-processing job diffs current vs. prior tables; stores summary JSON                                          |
| Catch news or enforcement actions you might otherwise miss | Press releases, RSS feeds, GDELT, sanctions lists                | Low-latency “news-harvester” hits feeds hourly; NLP tags each item with the correct manager & topic              |
| Keep your own notes, diligence memos, e-mails searchable   | Markdown/Text/PDF blobs                                          | Front-end upload widget; content dumped into same search index with access controls                              |

┌─────────────────┐
│  Scheduler      │   (Airflow / Prefect)
└─────────────────┘
        │ triggers DAGs
┌───────▼─────────┐
│  ETL Workers    │   (Python, Scrapy / requests-HTML / sec-api)
└───────┬─────────┘
        │ raw docs
┌───────▼─────────┐
│  Object Store   │   (S3 / MinIO) – PDFs, XBRL, CSV
└───────┬─────────┘
        │ parsed JSON
┌───────▼─────────┐
│  Postgres       │   relational core (managers, filings, hold-ings)
│  + pg_trgm/fts  │   full-text indices
└───────┬─────────┘
        │ vectors
┌───────▼─────────┐
│  Elastic / pgvecto│  semantic & keyword search
└───────┬─────────┘
        │ REST/GraphQL
┌───────▼─────────┐
│  UI / API       │   (Streamlit, Django, or Retool)
└─────────────────┘

3. Phase-by-phase plan
Phase 0 – Proof-of-concept (1-2 weeks)
Pick two managers you already follow.

Use the SEC EDGAR submissions JSON feed (https://data.sec.gov/submissions/CIK########.json) to pull their last 5 filings. 
sec.gov

Prototype a 13F parser with sec-api (free tier) to download holdings tables and extract CUSIP, shares, value. 
sec-api.io
sec-api.io

Store raw HTML + parsed tables in local SQLite; verify you can diff holdings quarter-to-quarter.

Bonus: feed one PDF annual report into Tika → text; save as blob.

Phase 1 – Production ETL (1-2 months)

| Task               | Recommended tooling                                                                           | Why                                              |
| ------------------ | --------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **Scheduler**      | *Prefect 2* (simpler) or *Airflow* (more knobs)                                               | DAGs, retries, alerting                          |
| **Filings ingest** | `sec-api` for US; Companies House REST for UK; fallback Scrapy spiders                        | Official endpoints reduce scraping headaches     |
| **News ingest**    | GDELT 2.0 API, RSS feeds via `feedparser`                                                     | Global coverage, near-real-time                  |
| **PDF/XBRL parse** | Apache Tika + `xbrl` Python libs                                                              | Uniform content extraction                       |
| **Storage**        | Postgres + S3 (or Azure Blob/GCS)                                                             | Cheap, battle-tested                             |
| **Search**         | Postgres FTS for “just works”; Elastic/OpenSearch if you need fuzzy or vector search at scale | Enables single search box across notes + filings |

Phase 2 – Analyst-facing portal (4-6 weeks)
Streamlit app for quick wins (forms, tables, charts in minutes).

Key pages:

Manager dashboard – latest holdings delta, filing timeline, news stream.

Universal search – type “XYZ Capital ESG policy” → returns filings, your memos, notes.

Upload widget – drag&drop your own memos; they’re indexed automatically.

Add role-based auth (Streamlit’s st_authenticator or behind-the-firewall).

Phase 3 – “AI-Assist” enhancements (optional / Q4 wish-list)
Vector-embed every doc with sentence-transformers; chat over your corpus with RAG.

Auto-summaries of new 13F: “Top new positions, largest exits”.

ESG or risk flagging with LLM classification.

4. Tech stack cheatsheet
   | Layer       | Minimalist choice                | Scales further                           |
| ----------- | -------------------------------- | ---------------------------------------- |
| Language    | Python 3.12                      | –                                        |
| Filings API | `sec-api` (US) ([sec-api.io][1]) | OpenEDGAR (self-host) ([arxiv.org][2])   |
| Scheduler   | Prefect                          | Airflow on K8s                           |
| DB          | Postgres 16                      | Postgres + Citus / Amazon RDS            |
| Search      | Postgres FTS                     | OpenSearch / Elastic 8                   |
| Storage     | Amazon S3                        | On-prem MinIO                            |
| UI          | Streamlit                        | React+FastAPI (if you outgrow Streamlit) |

[1]: https://sec-api.io/docs/sec-filings-render-api/python-example?utm_source=chatgpt.com "Download SEC Filings With Python"
[2]: https://arxiv.org/abs/1806.04973?utm_source=chatgpt.com "OpenEDGAR: Open Source Software for SEC EDGAR Analysis"

5. Governance & compliance quick hits
Respect robots.txt and SEC fair-use; throttle requests (the new EDGAR rate-limits >10 rps/IP).

Keep an audit log of every parsed field; if a parser misfires you need replayability.

Encrypt S3 bucket at rest; credentials via AWS IAM or Vault.

Add a one-click “forget this manager” routine for GDPR/contractual takedown.

6. Risks & mitigations
   | Risk                               | Mitigation                                                           |
| ---------------------------------- | -------------------------------------------------------------------- |
| API quota changes or paywalls      | Abstract each source behind an adaptor; swap out when terms change.  |
| Scraper breaks on HTML redesign    | Unit-test parsers nightly; keep prior HTML snapshots for regression. |
| Data explosion (filings are *big*) | Store raw docs once; zip older quarters; retain only diffs in DB.    |
| Colleagues accidentally drop DB    | Automated nightly S3 snapshot; terraform for re-provision.           |

 | Revised “Source Adapter” matrix
Region	Free endpoint you can hit today	What you actually get	Caveats & upgrade flags
US	SEC EDGAR Submissions JSON & Documents API	All filings + meta in real time	Free; 10 rps/IP hard-limit (it will 429). Paid sec-api tier buys higher throughput and historical search.
UK	Companies House REST API api.companieshouse.gov.uk	Full company register & PDF filings	Free key; soft 600 req/5 min cap. Paid feeds add daily bulk dumps and images 
developer.company-information.service.gov.uk
Canada	SEDAR+ public search (HTML, but stable)	PDFs / XBRLs for all issuers	No official API. Free scraping is fine for a dozen managers; heavy use → licence with CSA’s bulk feed 
sedarplus.ca
Australia	ASIC monthly CSV dumps on data.gov.au	Snapshots of company register	No filings PDFs; those are pay-per-doc. CSVs are free and good for status/addresses 
asic.gov.au
Singapore	MAS API (gov.sg)	Licences, enforcement, macro data	GA release Mar 2025, free key but 5 k req/day. Premium tier adds real-time feeds

Add one adapter.py per source. Each exposes the same three coroutine signatures:

python
Copy
Edit
async def list_new_filings(since: datetime) -> list[FilingMeta]:
async def download(filing: FilingMeta) -> bytes:
async def parse(raw: bytes) -> list[Dict[str, Any]]:
…so Prefect can call them in parallel without caring which jurisdiction it is.
If an adapter only yields a single parsed record, it should still return a
single-item list to keep the output contract consistent.

2 | Prototype topology (solo-maintainer friendly)
docker-compose.yml
├─ postgres      (13F tables, cost logs)
├─ minio         (S3-compatible object store)
├─ redis         (Prefect backend)
└─ etl           (your Python workers)

Prefect Cloud (free tier) handles scheduling + retry logic.

Streamlit container serves the analyst UI.

When you outgrow “solo dev”, drop Prefect into your own k8s cluster and swap Streamlit for React+FastAPI without touching the adapters.

3 | Cost & benefit telemetry
3.1 DB schema additions
CREATE TABLE api_usage (
    id           bigserial primary key,
    ts           timestamptz default now(),
    source       text,           -- 'edgar', 'companies_house', …
    endpoint     text,
    status       int,
    bytes        int,
    latency_ms   int,
    cost_usd     numeric(10,4)   -- 0 for free calls
);

CREATE MATERIALIZED VIEW monthly_usage AS
SELECT date_trunc('month', ts) AS month,
       source,
       count(*)        AS calls,
       sum(bytes)      AS mb,
       sum(cost_usd)   AS cost
FROM api_usage
GROUP BY 1,2;

3.2 Python helper
from contextlib import asynccontextmanager
import time, httpx, decimal
COST_PER_1K = {"sec_api_paid": decimal.Decimal("0.50")}  # future use

@asynccontextmanager
async def tracked_call(source, endpoint):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        latency = int((time.perf_counter() - t0)*1000)
        cost = decimal.Decimal("0")
        if source in COST_PER_1K:
            cost = COST_PER_1K[source] / 1000
        await db.execute(
            "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd)"
            "VALUES ($1,$2,$3,$4,$5,$6)",
            source, endpoint, resp.status_code, len(resp.content), latency, cost
        )

Drop that wrapper around every outbound request; the Streamlit “Admin” tab can simply SELECT * FROM monthly_usage.

Result: when the EDGAR 10-K parser suddenly eats 100 MB PDFs or SEDAR+ starts rate-limiting, you’ll see the spike. When sec-api’s paid tier chops latency from 4 s → 0.8 s you’ll have numbers to justify the invoice.

4 | Sprint backlog (next 4–6 weeks)
| Week               | Deliverable                                                                                 | Notes                                               |
| ------------------ | ------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| **1**              | Docker compose up; EDGAR adapter live; cost table logging                                   | Local SQLite OK at first                            |
| **2**              | Companies House + SEDAR+ adapters; Streamlit “Manager search” page                          | Use UK PDF endpoints for accounts                   |
| **3**              | MAS + ASIC adapters; holdings diff view; monthly\_usage view                                | Quick-n-dirty HTML scrape for MAS if API quota nags |
| **4**              | Automated email digest (“last 24 h filings & news”)                                         | Prefect task +                                      |
| SendGrid free tier |                                                                                             |                                                     |
| **5**              | Security hardening (S3 encryption, IAM roles)                                               | Prep for cloud migration                            |
| **6**              | Write up *ROI playbook* – table of response times, data coverage, costs vs. premium options | Artifact to show management                         |

5 | Forward-compatibles you don’t want to re-engineer later
Abstract storage – everything hits storage.put(blob, key=hash). Today it’s MinIO, tomorrow it’s S3, on-prem NFS, or an Iceberg lakehouse.

Schema versioning – store schema_version with each parsed JSON so you can replay old docs if parsers improve.

IAM first – even in free-tier land, use environment-variable secrets + least privilege. Your future cybersecurity auditors will thank you.

6 | Potential potholes
Gotcha	Early antidote
Overseas filings often arrive as scanned images → useless text	Wire in Tesseract OCR now; flag >20 % image-only docs for manual QC
Date formats pile up (31-12-2024 vs 12-31-24)	Normalise to ISO in the adapter layer, not downstream
One-man show risk	Add a README_bootstrap.md + docker-compose --profile dev to let a teammate spin up the stack in 10 min


---

### 🗓 Milestones & Deadlines

| Milestone ID | Goal                                                                     | Due Date   |
|--------------|--------------------------------------------------------------------------|------------|
| **M1**       | **Stack & EDGAR POC**<br>– `docker compose up` with Postgres, MinIO, Prefect<br>– Basic EDGAR adapter writes to MinIO & PG<br>– API-usage telemetry logging | 2025-06-13 |
| **M2**       | **UK & Canada Adapters + UI Stub**<br>– Companies House & SEDAR+ adapters<br>– Streamlit “Manager search” page scaffold              | 2025-06-20 |
| **M3**       | **APAC Adapters & Holdings Diff**<br>– MAS & ASIC adapters<br>– Holdings-diff view and monthly_usage materialized view         | 2025-06-27 |
| **M4**       | **Automated Digest & CI/CD**<br>– Prefect email digest via SendGrid<br>– Basic CI pipeline for building ETL Docker image        | 2025-07-04 |
| **M5**       | **Security & Cloud Prep**<br>– S3 encryption + IAM roles configured<br>– `README_bootstrap.md` onboarding doc & cloud-migration plan | 2025-07-11 |
| **M6**       | **ROI Playbook & Handoff**<br>– Cost-benefit report for free vs. paid tiers<br>– Finalize roadmap & handoff docs                  | 2025-07-18 |

---

### 🔨 How to import into GitHub

1. **Milestones** → New milestone for each row above (copy title & due date).  
2. **Issues** → Create template issues linked to each milestone (e.g. “Implement EDGAR adapter”, “Build Streamlit search page”).  
3. **Project Board** → Add columns for To-Do, In Progress, Done and drag issues under their milestone.  

---

### 🤔 Next steps

- Make sure your repo has `docker-compose.yml`, `etl/`, `adapters/`, `README_bootstrap.md`.  
- Kick off **M1** by tagging **v0.1** once you’ve got the POC running.  
- Feel free to tweak deadlines if you hit unexpected SEC rate-limits or holiday weekends (e.g., July 4).

Let me know if you want issue templates or GitHub Action snippets to automate any of the above!

| Day            | Deliverable                  | Concrete tasks & commands                                                                                                                                                                                                                                                                                                                                                                       |
| -------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **D-0 (prep)** | *Project skeleton & Git*     | 1. `mkdir manager-intel && cd manager-intel`  <br>2. `git init && gh repo create` (or GitLab). <br>3. Create `README_bootstrap.md` with one-liner start-up instructions—future you will thank present you.                                                                                                                                                                                      |
| **D-1**        | *Tooling on your laptop*     | *Install*<br>• Python 3.12 via pyenv (keeps your system Python clean). <br>• Docker Desktop + the Compose v2 plugin. <br>• VS Code + Python & Docker extensions. <br>Verify: `docker --version`, `docker compose version`, `python -m pip --version`.                                                                                                                                           |
| **D-2**        | *Local “data stack” up*      | 1. Write a **`docker-compose.yml`** with three services (trimmed example below). <br>2. `docker compose up -d` then hit `http://localhost:9001`—MinIO console should load (default port pair 9000/9001). ([medium.com][1])                                                                                                                                                                      |
| **D-3**        | *Postgres schema + cost log* | 1. `docker exec -it db psql -U postgres` <br>Run: `sql CREATE TABLE api_usage (id bigserial PRIMARY KEY, ts timestamptz DEFAULT now(), source text, endpoint text, status int, bytes int, latency_ms int, cost_usd numeric(10,4));` <br>2. Create a second file `schema.sql` in repo + commit.                                                                                                  |
| **D-4**        | *Scheduler stub (Prefect)*   | 1. `pip install "prefect>=2.18"` inside a **`etl`** virtualenv. <br>2. `prefect cloud login` (free tier) then `prefect deployment build flows/edgar_flow.py:edgar -n dev && prefect deployment apply`. <br>Docker alternative: clone the community `prefect-docker-compose` repo and copy its Compose snippet if you prefer *everything* containerised. ([github.com][2])                       |
| **D-5**        | *Minimal EDGAR adapter*      | In `adapters/edgar.py`:  <br>`python\nBASE='https://data.sec.gov/submissions/{cik}.json'\n`  <br>• Fetch Apple’s CIK (`0000320193`) as smoke-test. <br>• Dump raw JSON to MinIO bucket `filings/raw/`. <br>• Parse the top-level `filings.recent` arrays into Postgres table `filings`.  <br>SEC’s submissions feed is public & unauthenticated—just respect the 10 req/s limit. ([sec.gov][3]) |
| **D-6**        | *Cost-telemetry wrapper*     | Add the `tracked_call()` context manager around every HTTP request; log to `api_usage`.  Run a flow; `SELECT * FROM api_usage LIMIT 5;` should show non-zero `latency_ms`.                                                                                                                                                                                                                      |
| **D-7**        | *Smoke test & retro*         | • Tag repo `v0.1`.  <br>• Open three GitHub Issues: *“Improve Holdings diff”*, *“Companies House adapter”*, *“Streamlit dashboard stub”*.  <br>• Write a ½-page retro: what hurt, what worked.                                                                                                                                                                                                  |

[1]: https://medium.com/%40randy.hamzah.h/running-minio-server-with-docker-compose-54bab3afbe31 "Running Minio Server with Docker Compose | by Randy Hardianto | Medium"
[2]: https://github.com/rpeden/prefect-docker-compose "GitHub - rpeden/prefect-docker-compose: A repository that makes it easy to get up and running with Prefect 2 using Docker Compose."
[3]: https://www.sec.gov/search-filings/edgar-application-programming-interfaces "SEC.gov | EDGAR Application Programming Interfaces (APIs)"

version: "3.9"
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: postgres
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]

  minio:
    image: minio/minio:RELEASE.2025-05-24T17-08-30Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
    ports: ["9000:9000", "9001:9001"]
    volumes: ["miniodata:/data"]

  etl:
    build: ./etl     # Dockerfile uses python:3.12-slim
    volumes:
      - ./etl:/code
    depends_on: [db, minio]

volumes:
  pgdata:
  miniodata:

Immediate priorities - When the user needs to take action locally before you can move forward, please notify
| Component                   | Purpose                    | “Later enterprise” path                        |
| --------------------------- | -------------------------- | ---------------------------------------------- |
| **Git repo**                | Version control, issues    | Move to private GitHub Org / Azure DevOps      |
| **Docker + Compose**        | One-command local stack    | Promote services to Kubernetes via Helm charts |
| **Postgres 16**             | Relational core, cost logs | Scale-out → Amazon RDS or on-prem Citus        |
| **MinIO (S3-compatible)**   | Raw PDFs, JSON blobs       | Swap to AWS S3 / object-store of choice        |
| **Prefect Cloud (free)**    | Orchestration, retries     | Self-hosted Prefect Server inside k8s          |
| **ETL image**               | Houses adapters & flows    | CI pipeline builds & pushes to your registry   |
| **tracked\_call() logging** | Shows value of paid APIs   | Feed Grafana dashboards for management         |
