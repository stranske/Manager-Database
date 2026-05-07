from pathlib import Path

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "schema-idempotence.yml"
SCHEMA_PATHS = [
    "schema.sql",
    "scripts/verify_schema_idempotence.sh",
    ".github/workflows/schema-idempotence.yml",
]


def load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_schema_idempotence_workflow_runs_verifier_against_pg16_service():
    workflow = load_workflow()
    job = workflow["jobs"]["schema-verifier"]
    service = job["services"]["postgres"]
    run_step = job["steps"][-1]

    assert service["image"] == "pgvector/pgvector:pg16"
    assert run_step["env"] == {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "postgres",
    }
    assert run_step["run"] == "bash scripts/verify_schema_idempotence.sh"


def test_schema_idempotence_workflow_runs_on_schema_changes_and_schedule():
    workflow = load_workflow()
    triggers = workflow[True]

    assert triggers["push"]["branches"] == ["main"]
    assert triggers["push"]["paths"] == SCHEMA_PATHS
    assert triggers["pull_request"]["paths"] == SCHEMA_PATHS
    assert triggers["schedule"] == [{"cron": "30 4 * * *"}]
    assert triggers["workflow_dispatch"] is None
