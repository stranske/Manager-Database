"""The migration graph must stay linear with exactly one head.

Two feature branches numbered their migration from the same parent, so `020`
was declared twice on `main` and every `alembic upgrade head` raised
`MultipleHeads: 020, 020` (CI run 30655077499). Nothing failed until the second
branch merged, because each PR only ever saw its own revision id. These checks
read the revision map directly, so a collision fails on the branch that
introduces it rather than on `main` after the merge.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "alembic" / "versions"


def _script_directory() -> ScriptDirectory:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    return ScriptDirectory.from_config(config)


def _declared_revision(path: Path) -> str | None:
    """Read the module-level `revision` literal without importing the migration."""
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "revision":
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    return node.value.value
    return None


def test_revision_identifiers_are_unique():
    """Parse the files rather than the revision map, which keys by id and hides collisions."""
    by_revision: dict[str, list[str]] = defaultdict(list)
    for path in sorted(VERSIONS.glob("[0-9]*.py")):
        revision = _declared_revision(path)
        assert revision is not None, f"{path.name} declares no module-level revision id"
        by_revision[revision].append(path.name)

    duplicates = {rev: files for rev, files in by_revision.items() if len(files) > 1}

    assert not duplicates, (
        f"duplicate alembic revision identifier(s): {duplicates}. "
        "Two migrations claim the same revision id, so alembic cannot resolve "
        "the upgrade path. Renumber the migration that merged later."
    )


def test_migration_graph_has_exactly_one_head():
    heads = _script_directory().get_heads()

    assert len(heads) == 1, (
        f"alembic has {len(heads)} heads ({sorted(heads)}), expected exactly one. "
        "A new migration must set down_revision to the current head; rebase and "
        "renumber if another migration landed first."
    )


def test_every_migration_reaches_the_base():
    script_directory = _script_directory()
    (head,) = script_directory.get_heads()

    walked = {script.revision for script in script_directory.walk_revisions("base", head)}
    orphans = sorted(
        script.revision
        for script in script_directory.walk_revisions()
        if script.revision not in walked
    )

    assert not orphans, (
        f"migration(s) {orphans} are not reachable from base..{head}; "
        "their down_revision points outside the main chain."
    )
