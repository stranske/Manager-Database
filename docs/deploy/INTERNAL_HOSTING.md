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

```bash
# On the internal host, from the repo root:
docker compose up -d db minio api ui

# Confirm the API is healthy before exposing the UI URL:
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
```

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
