## 📊 Manager-Intel Platform

A lightweight, extensible system to crawl public filings (13Fs, annual reports, overseas registries), parse and diff holdings, harvest news, and serve it all via a searchable UI. Built solo in Docker today, enterprise-ready tomorrow.

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

