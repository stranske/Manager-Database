from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_compose_exposes_api_service_for_local_stack() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "\n  api:\n" in compose
    assert "uvicorn api.chat:app --host 0.0.0.0 --port 8000" in compose
    assert "dockerfile: api/Dockerfile" in compose
    assert '- "8000:8000"' in compose


def test_bootstrap_readme_uses_compose_stack_path() -> None:
    readme = (REPO_ROOT / "README_bootstrap.md").read_text(encoding="utf-8")
    assert "docker compose up -d db minio api ui" in readme
    assert "uvicorn api.chat:app --reload" not in readme
