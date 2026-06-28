"""Guard sparse-checkout blocks that load github-api-with-retry.js."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
SCRIPTS_DIR = ROOT / ".github" / "scripts"

RETRY_SCRIPT = ".github/scripts/github-api-with-retry.js"
CLASSIFIER_SCRIPT = ".github/scripts/error_classifier.js"
SPARSE_CHECKOUT_BLOCK = re.compile(
    r"sparse-checkout:\s*\|\s*\n((?:[ \t]+[^\n]+\n)+)",
    re.MULTILINE,
)


def _sparse_checkout_paths(block: str) -> list[str]:
    return [
        line.strip()
        for line in block.splitlines()
        if line.strip() and not line.strip().startswith("sparse-checkout-cone-mode")
    ]


def _iter_sparse_checkout_blocks(workflow_text: str) -> list[list[str]]:
    return [_sparse_checkout_paths(match.group(1)) for match in SPARSE_CHECKOUT_BLOCK.finditer(workflow_text)]


def test_sparse_checkouts_include_error_classifier_with_retry_helper() -> None:
    offenders: list[str] = []

    for workflow_path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        text = workflow_path.read_text(encoding="utf-8")
        for index, paths in enumerate(_iter_sparse_checkout_blocks(text), start=1):
            if RETRY_SCRIPT in paths and CLASSIFIER_SCRIPT not in paths:
                offenders.append(f"{workflow_path.name} block {index}: {paths}")

    assert not offenders, (
        "github-api-with-retry.js requires ./error_classifier at module load; "
        "add error_classifier.js to sparse-checkout:\n"
        + "\n".join(offenders)
    )


def test_github_api_retry_imports_with_minimal_sparse_checkout(tmp_path: Path) -> None:
    """Simulate the Record autofix metrics checkout and require the helper."""
    checkout_root = tmp_path / "repo"
    scripts_dir = checkout_root / ".github" / "scripts"
    scripts_dir.mkdir(parents=True)

    for script_name in ("error_classifier.js", "github-api-with-retry.js"):
        shutil.copy2(SCRIPTS_DIR / script_name, scripts_dir / script_name)

    result = subprocess.run(
        [
            "node",
            "-e",
            "require('./.github/scripts/github-api-with-retry.js');",
        ],
        cwd=checkout_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_record_autofix_metrics_job_sparse_checkout_paths() -> None:
    workflow = yaml.safe_load((WORKFLOWS_DIR / "agents-81-gate-followups.yml").read_text(encoding="utf-8"))
    checkout_step = workflow["jobs"]["metrics"]["steps"][0]
    paths = _sparse_checkout_paths(checkout_step["with"]["sparse-checkout"])

    assert RETRY_SCRIPT in paths
    assert CLASSIFIER_SCRIPT in paths
