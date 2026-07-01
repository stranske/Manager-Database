from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PRODUCTION_ENV_PATHS = (
    "adapters",
    "alerts",
    "api",
    "chains",
    "etl",
    "llm",
    "ui",
    "embeddings.py",
    "profiler.py",
    "scripts/db_snapshot_restore.py",
    "tools/embedding_provider.py",
)

# CI/workflow-only, test-only, or derived aliases that should not be presented
# as operator-facing application configuration in .env.example.
INTERNAL_ENV_ALLOWLIST = {
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_API_VERSION",
    "AZURE_OPENAI_ENDPOINT",
    "CLAUDE_API_STRANSKE",
}


class EnvVarVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "getenv"
            and isinstance(func.value, ast.Name)
            and func.value.id == "os"
        ):
            self._add_constant_name(node)
        elif (
            isinstance(func, ast.Attribute)
            and func.attr == "get"
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "environ"
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "os"
        ):
            self._add_constant_name(node)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == "environ"
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "os"
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            self.names.add(node.slice.value)
        self.generic_visit(node)

    def _add_constant_name(self, node: ast.Call) -> None:
        if (
            node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            self.names.add(node.args[0].value)


def _python_files(paths: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for relative in paths:
        path = ROOT / relative
        if path.is_file():
            files.append(path)
        else:
            files.extend(sorted(path.rglob("*.py")))
    return files


def _env_reads(files: list[Path]) -> set[str]:
    names: set[str] = set()
    for path in files:
        visitor = EnvVarVisitor()
        visitor.visit(ast.parse(path.read_text(), filename=str(path)))
        names.update(visitor.names)
    return names


def _documented_env_names(env_text: str) -> set[str]:
    names: set[str] = set()
    for raw_line in env_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        names.add(line.split("=", 1)[0].strip())
    return names


def test_env_example_documents_production_env_reads() -> None:
    documented = _documented_env_names((ROOT / ".env.example").read_text())
    consumed = _env_reads(_python_files(PRODUCTION_ENV_PATHS))

    missing = sorted(consumed - documented - INTERNAL_ENV_ALLOWLIST)

    assert missing == []


def test_env_coverage_guard_catches_new_undocumented_variable(tmp_path: Path) -> None:
    module = tmp_path / "new_consumer.py"
    module.write_text('import os\nVALUE = os.getenv("FOO_NEW")\n')

    consumed = _env_reads([module])
    missing = consumed - _documented_env_names("DB_URL=sqlite:///dev.db\n") - INTERNAL_ENV_ALLOWLIST
    assert missing == {"FOO_NEW"}

    documented = _documented_env_names("DB_URL=sqlite:///dev.db\nFOO_NEW=\n")
    assert consumed - documented - INTERNAL_ENV_ALLOWLIST == set()
