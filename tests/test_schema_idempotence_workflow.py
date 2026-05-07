from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "schema-idempotence.yml"


def test_schema_idempotence_workflow_runs_verifier_against_pg16_service():
    workflow = WORKFLOW.read_text()

    assert "schema-verifier:" in workflow
    assert "image: pgvector/pgvector:pg16" in workflow
    assert "PGHOST: localhost" in workflow
    assert "PGPORT: '5432'" in workflow
    assert "PGUSER: postgres" in workflow
    assert "bash scripts/verify_schema_idempotence.sh" in workflow


def test_schema_idempotence_workflow_runs_on_schema_changes_and_schedule():
    workflow = WORKFLOW.read_text()

    assert "push:" in workflow
    assert "pull_request:" in workflow
    assert "schedule:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "schema.sql" in workflow
    assert "scripts/verify_schema_idempotence.sh" in workflow
