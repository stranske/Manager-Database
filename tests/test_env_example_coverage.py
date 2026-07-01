from __future__ import annotations

import ast
import re
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

ENV_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]+$")

# CI/workflow-only, test-only, or derived aliases that should not be presented
# as operator-facing application configuration in .env.example.
INTERNAL_ENV_ALLOWLIST: set[str] = set()


class EnvVarVisitor(ast.NodeVisitor):
    def __init__(self, constants: dict[str, str]) -> None:
        self.constants = constants
        self.names: set[str] = set()

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "getenv"
            and isinstance(func.value, ast.Name)
            and func.value.id == "os"
        ):
            self._add_env_name(node)
        elif (
            isinstance(func, ast.Attribute)
            and func.attr in {"get", "setdefault"}
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "environ"
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "os"
        ):
            self._add_env_name(node)
        elif self._looks_like_env_wrapper(node):
            self._add_env_name(node)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == "environ"
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "os"
        ):
            self._add_name_expr(node.slice)
        self.generic_visit(node)

    def _add_env_name(self, node: ast.Call) -> None:
        if node.args:
            self._add_name_expr(node.args[0])

    def _add_name_expr(self, expr: ast.expr) -> None:
        value = self._resolve_name_expr(expr)
        if value is not None:
            self.names.add(value)

    def _resolve_name_expr(self, expr: ast.expr) -> str | None:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return expr.value
        if isinstance(expr, ast.Name):
            return self.constants.get(expr.id)
        return None

    def _looks_like_env_wrapper(self, node: ast.Call) -> bool:
        if not node.args:
            return False
        if isinstance(node.func, ast.Name) and node.func.id.startswith("_env"):
            return self._is_env_name_expr(node.args[0])
        return False

    def _is_env_name_expr(self, expr: ast.expr) -> bool:
        value = self._resolve_name_expr(expr)
        return bool(value and ENV_NAME_PATTERN.fullmatch(value))


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
        tree = ast.parse(path.read_text(), filename=str(path))
        visitor = EnvVarVisitor(_module_string_constants(tree))
        visitor.visit(tree)
        names.update(visitor.names)
    return names


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    constants: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            value = _string_constant(node.value)
            if value is None:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            value = _string_constant(node.value)
            if value is not None:
                constants[node.target.id] = value
    return constants


def _string_constant(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


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
    module.write_text(
        'import os\nDIRECT = os.getenv("FOO_NEW")\n'
        'ENV_CONST = "BAR_NEW"\nCONST = os.environ.get(ENV_CONST)\n'
        'def _env_int(name: str) -> int:\n    return int(os.environ.get(name, "1"))\n'
        "WRAPPED = _env_int(ENV_CONST)\n"
    )

    consumed = _env_reads([module])
    missing = consumed - _documented_env_names("DB_URL=sqlite:///dev.db\n") - INTERNAL_ENV_ALLOWLIST
    assert missing == {"BAR_NEW", "FOO_NEW"}

    documented = _documented_env_names("DB_URL=sqlite:///dev.db\nFOO_NEW=\nBAR_NEW=\n")
    assert consumed - documented - INTERNAL_ENV_ALLOWLIST == set()
