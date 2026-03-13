import shutil
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.root_discovery_service import discover_and_sync_roots

FIXTURE_PATH = Path("tests/fixtures/root_discovery/sample_tree_manifest.txt")


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "root-discovery-sample.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_root_discovery_syncs_sanitized_sample_tree(db_session: Session, tmp_path: Path) -> None:
    raid_path = tmp_path / "raid_sample"
    _materialize_tree_manifest(FIXTURE_PATH, raid_path)

    expected_top_level_paths = tuple(
        sorted(
            (
                (raid_path / "MOVIE").resolve(strict=False).as_posix(),
                (raid_path / "SHORTS").resolve(strict=False).as_posix(),
                (raid_path / "TV SHOW").resolve(strict=False).as_posix(),
                (raid_path / "TV SHOW_SUB").resolve(strict=False).as_posix(),
                (raid_path / "СЕРИАЛЫ").resolve(strict=False).as_posix(),
            )
        )
    )

    first_result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_sample",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert tuple(record.path for record in first_result.created) == expected_top_level_paths
    assert tuple(record.path for record in first_result.discovered) == expected_top_level_paths
    assert all(record.needs_structural_rescan is True for record in first_result.created)
    assert first_result.reactivated == ()
    assert first_result.marked_missing == ()
    assert first_result.unchanged_present == ()

    repo = RootsRepository(db_session)
    assert [record.path for record in repo.list_all()] == list(expected_top_level_paths)

    second_result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_sample",
        raid_path=raid_path,
        discovered_at="2026-03-13T11:00:00+00:00",
    )

    assert second_result.created == ()
    assert second_result.marked_missing == ()
    assert second_result.reactivated == ()
    assert tuple(record.path for record in second_result.discovered) == expected_top_level_paths
    assert (
        tuple(record.path for record in second_result.unchanged_present)
        == expected_top_level_paths
    )
    assert len(repo.list_all()) == len(expected_top_level_paths)

    removed_root_path = (raid_path / "TV SHOW_SUB").resolve(strict=False).as_posix()
    shutil.rmtree(raid_path / "TV SHOW_SUB")

    third_result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_sample",
        raid_path=raid_path,
        discovered_at="2026-03-13T12:00:00+00:00",
    )

    assert [record.path for record in third_result.marked_missing] == [removed_root_path]
    assert sorted(record.path for record in third_result.discovered) == [
        path for path in expected_top_level_paths if path != removed_root_path
    ]
    missing_root = repo.get_by_path(removed_root_path)
    assert missing_root is not None
    assert missing_root.is_missing is True
    assert len(repo.list_all()) == len(expected_top_level_paths)


def _materialize_tree_manifest(manifest_path: Path, raid_path: Path) -> None:
    raid_path.mkdir(parents=True, exist_ok=True)

    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        entry_type, relative_path = line.split(" ", 1)
        target_path = raid_path / relative_path
        if entry_type == "d":
            target_path.mkdir(parents=True, exist_ok=True)
            continue
        if entry_type == "f":
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text("sample fixture\n", encoding="utf-8")
            continue
        raise AssertionError(f"Unsupported manifest entry: {line}")
