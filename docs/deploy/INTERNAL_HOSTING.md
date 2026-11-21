# Internal / on-prem hosting for the analyst UI

This guide stands up the existing `docker-compose.yml` stack on an **internal,
org-perimeter host** so analysts reach the full Manager-Database UI from a
browser with no per-user install, while proprietary data (Postgres, MinIO, API)
never leaves the perimeter and the external-LLM boundary is explicitly gated.

> Scope: a single-org internal instance protected by the existing
> `UI_USERNAME`/`UI_PASSWORD` gate (`ui/__init__.py`). Production multi-tenant
> SSO is out of scope.

## What runs where

The compose stack (`docker-compose.yml`) already defines every service needed:

| Service | Image / command | Port | Role |
| ------- | --------------- | ---- | ---- |
| `db`    | `pgvector/pgvector:pg16` | 5432 | Postgres data plane (stays internal) |
| `minio` | object store | 9000 | Document/object storage (stays internal) |
| `api`   | `uvicorn api.chat:app` | 8000 | Deterministic + chat API |
| `ui`    | Streamlit | 8501 | Analyst browser UI |

The `api` service has a healthcheck and the deterministic routes
(`/managers`, `/health/detailed`, `/signals`, `/activism`, `/search`) carry **no
LLM dependency** — they run on real data safely.

## Do NOT host on external SaaS

Proprietary manager data must not egress. **Do not** deploy to Streamlit
Community Cloud, Hugging Face Spaces, Render, Fly, Railway, or any external /
community SaaS. Use an internal target where you control the network, e.g.:

- an internal VM / bare-metal host reachable only on the corporate network,
- an internal container platform (Posit Connect, internal Azure Container Apps,
  an on-prem Kubernetes namespace),

with Postgres, MinIO, and the API all bound to internal addresses and the UI
reachable via an **internal URL** (optionally behind the org reverse proxy / VPN).

### Stand up the stack

On a **fresh** Postgres `pgdata` volume, `db` automatically applies `schema.sql` (already at head) on first start:

```bash
# Fresh volume on the internal host, from the repo root:
docker compose up -d db minio api ui

# Confirm the API is healthy before exposing the UI URL:
curl -fsS http://<internal-host>:8000/health/detailed
```

#### Upgrading an existing PostgreSQL volume (migration 026)

Postgres only runs `/docker-entrypoint-initdb.d/schema.sql` on an empty `pgdata` volume. Before starting `api` (`uvicorn`) against an **existing** volume, apply Alembic revision `026` (`api_usage.cost_usd` `NUMERIC(12, 6)` and updated `monthly_usage` materialized view) so usage writes are never rounded or miscounted:

```bash
# 1. Stop API/UI (if running) and ensure only Postgres is up
docker compose stop api ui
docker compose up -d db

# 2. Check whether the volume already tracks Alembic revisions
docker compose exec -T db psql -U postgres -d postgres -c \
  "SELECT to_regclass('alembic_version') AS alembic_version_table;"
```

- **Alembic-managed volume** (`alembic_version` exists and `alembic current` reports a revision):
  ```bash
  docker compose run --rm --no-deps api python -m alembic current
  docker compose run --rm --no-deps api python -m alembic upgrade head
  docker compose run --rm --no-deps api python -m alembic current   # expect: 026 (head)
  ```
- **`schema.sql`-initialized volume** (`alembic_version` is absent or empty): establish the actual schema baseline **before** any `alembic stamp` so intermediate revisions are never skipped:
  ```bash
  docker compose exec -T db psql -U postgres -d postgres -c "
  SELECT column_name, numeric_precision, numeric_scale
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = 'api_usage' AND column_name = 'cost_usd';
  SELECT definition FROM pg_matviews
  WHERE schemaname = 'public' AND matviewname = 'monthly_usage';
  SELECT table_name, column_name
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND (
      (table_name = 'backtest_results' AND column_name IN (
        'source_filing_id',
        'source_holding_id',
        'event_period',
        'source_knowledge_time',
        'applied_cutoff'
      ))
      OR (table_name = 'activism_campaigns' AND column_name = 'opening_filing_id')
      OR (table_name = 'manager_deletion_operations' AND column_name = 'operation_id')
    )
  ORDER BY table_name, column_name;
  "
  ```
  - Only after confirming the complete revision-`025` baseline (all five `backtest_results` provenance columns, `activism_campaigns.opening_filing_id`, `manager_deletion_operations`, and no missing tables/columns from earlier revisions in `alembic/versions/`):
    - If `cost_usd` is `(10, 4)` (or `monthly_usage` still sums `bytes` directly), stamp `025` and upgrade:
      ```bash
      docker compose run --rm --no-deps api python -m alembic stamp 025
      docker compose run --rm --no-deps api python -m alembic upgrade head
      ```
    - If `cost_usd` is already `(12, 6)` and `monthly_usage` already excludes `'langchain'` / negative `bytes`, stamp `026` (`docker compose run --rm --no-deps api python -m alembic stamp 026`).
  - If any pre-`026` object is missing, compare `alembic/versions/` against the live catalog to find the highest revision whose complete schema object set is present, stamp that verified baseline (`docker compose run --rm --no-deps api python -m alembic stamp <verified_revision>`), and run `docker compose run --rm --no-deps api python -m alembic upgrade head`.

```bash
# 3. Confirm 026 is active and refresh monthly_usage before starting uvicorn
docker compose run --rm --no-deps api python -m alembic current
docker compose exec -T db psql -U postgres -d postgres -c "
SELECT numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'api_usage' AND column_name = 'cost_usd';
REFRESH MATERIALIZED VIEW monthly_usage;
"

# 4. Start uvicorn and the rest of the stack
docker compose up -d minio api ui
curl -fsS http://<internal-host>:8000/health/detailed
```

Expose `:8501` (UI) and `:8000` (API) only on the internal network. Set
`CHAT_API_URL`/`CHAT_FEEDBACK_URL` for the `ui` service to the internal API
address if it differs from the compose default.

### Enforce auth on the hosted instance

`require_login()` (`ui/__init__.py`) gates the UI. For the hosted instance set
real credentials (no dev-bypass warning). `docker-compose.yml` passes these
host environment variables into the `ui` container:

```bash
export UI_USERNAME="<analyst-username>"
export UI_PASSWORD="<strong-password>"
export UI_COOKIE_KEY="<independent-random-secret-at-least-32-characters>"
```

Generate `UI_COOKIE_KEY` from cryptographically secure random bytes and keep it
independent of the login password and `CHAT_SESSION_COOKIE_SECRET`. Configured
login fails closed if it is missing or shorter than 32 characters. Rotate it
alongside `UI_PASSWORD` when existing UI reauthentication cookies must be
invalidated. The password hash is cached across reruns and replaced when the
configured password changes.

## LLM boundary: the `LLM_ZONE` switch

The chat (Research) page is the **only** external-LLM boundary. The provider
chokepoint is `_build_chat_client_info()` / `api/chat.py`. Pick exactly one of
two zones for an internal deployment.

### Option A — chat disabled (default-safe)

Set the zone to `disabled`. The deterministic pages keep running on real data;
`POST /api/chat` returns a **structured 200 notice** (`chat_disabled: true`)
instead of attempting a provider call or raising 503, and the Research page
renders a clear *"Research chat is disabled in this internal zone"* message
instead of a crash.

```bash
export LLM_ZONE=disabled
```

`docker-compose.yml` passes `LLM_ZONE` into the `api` container and defaults it
to `disabled` when the host variable is not set.

### Option B — authorized, no-train endpoint

Only if your org has an **authorized, no-train** provider endpoint, point the
chat client at it instead of disabling chat:

- Configure the provider base URL/model via the existing `llm/` config
  (`llm/client.py`, `config/model_registry.json`) consumed by
  `_build_chat_client_info()` (`api/chat.py`).
- Provide the credential (`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`) for that
  authorized endpoint **only**.
- Redaction / prompt-injection guards in `llm/injection.py` continue to apply to
  chat input; do not bypass them.

> If a provider key is **not** configured and `LLM_ZONE` is **not** `disabled`,
> the chat route still returns 503. Prefer `LLM_ZONE=disabled` for any internal
> instance without an authorized endpoint so the UI degrades cleanly.

## Live-verification gate

`scripts/readiness_smoke.py` probes `/health/detailed`, `/managers`, `/chat`,
and UI reachability and is designed to run **without provider credentials**. Run
it against the internal host and capture an exit-0 result as the deploy evidence:

```bash
# Point it at the internal host (see the script's --help / env for the base URL),
# then:
python scripts/readiness_smoke.py
echo "readiness_smoke exit: $?"   # must be 0
```

A captured `readiness_smoke exit: 0` against the internal URL — together with the
`LLM_ZONE=disabled` Research-page notice screenshot — is the acceptance evidence
for an internal deployment.

## Perimeter statement

With this setup, Postgres, MinIO, and the API stay **inside the org perimeter**.
No proprietary data is sent to external SaaS or to an unauthorized LLM: chat is
either disabled (`LLM_ZONE=disabled`) or bound only to an org-authorized,
no-train endpoint.

## Backup and restore

Postgres deployments must also configure the nightly encrypted snapshot workflow
described in `docs/runbooks/database-backup-restore.md`. Capture one successful
snapshot workflow run and one restore dry-run before treating a hosted instance
as recoverable.
