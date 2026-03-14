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
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.structural_scan_service import scan_root_structure
from backup_projects.services.structural_scan_sync_service import sync_structural_scan_result

FIXTURE_PATH = Path("tests/fixtures/structural_scan/sample_tree_manifest.txt")


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "structural-sample.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_structural_scan_and_sync_on_sanitized_sample_tree(
    db_session: Session,
    tmp_path: Path,
) -> None:
    root_path = tmp_path / "sample_root"
    _materialize_tree_manifest(FIXTURE_PATH, root_path)

    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = roots_repo.create(
        raid_name="raid_sample",
        name="sample_root",
        path=root_path.resolve().as_posix(),
        device_id=100,
        inode=200,
        mtime_ns=300,
        ctime_ns=400,
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    allowed_extensions = {"prproj", "aep", "drp", "aaf", "xml"}
    first_scan_result = scan_root_structure(
        root_path=root_path,
        allowed_extensions=allowed_extensions,
    )

    assert first_scan_result.root_path == root_path.resolve().as_posix()
    assert [project_dir.relative_path for project_dir in first_scan_result.project_dirs] == [
        "Mixed Suite",
        "Neutral Exchange",
        "Premiere Only",
        "Resolve Only",
    ]
    assert {
        project_dir.relative_path: project_dir.dir_type
        for project_dir in first_scan_result.project_dirs
    } == {
        "Mixed Suite": "mixed",
        "Neutral Exchange": "unknown",
        "Premiere Only": "premiere",
        "Resolve Only": "resolve",
    }

    mixed_scan_dir = first_scan_result.project_dirs[0]
    assert {file.relative_path for file in mixed_scan_dir.files} == {
        "Autosaves/edit_autosave.prproj",
        "Cache/render.tmp",
        "Nested AE/comp.aep",
        "edit.prproj",
    }
    assert "Mixed Suite/Nested AE" not in [
        project_dir.relative_path for project_dir in first_scan_result.project_dirs
    ]

    first_sync_result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=first_scan_result,
        synced_at="2026-03-14T10:00:00+00:00",
    )

    assert first_sync_result.scanned_project_dir_count == 4
    assert first_sync_result.created_project_dir_count == 4
    assert first_sync_result.scanned_project_file_count == 8
    assert first_sync_result.created_project_file_count == 8

    synced_dirs = {record.relative_path: record for record in dirs_repo.list_by_root(root.id)}
    assert sorted(synced_dirs) == [
        "Mixed Suite",
        "Neutral Exchange",
        "Premiere Only",
        "Resolve Only",
    ]
    assert all(record.is_missing is False for record in synced_dirs.values())
    assert synced_dirs["Mixed Suite"].dir_type == "mixed"
    assert synced_dirs["Neutral Exchange"].dir_type == "unknown"
    assert "Mixed Suite/Nested AE" not in synced_dirs

    mixed_db_files = files_repo.list_by_project_dir(synced_dirs["Mixed Suite"].id)
    assert [record.relative_path for record in mixed_db_files] == [
        "Mixed Suite/Autosaves/edit_autosave.prproj",
        "Mixed Suite/Cache/render.tmp",
        "Mixed Suite/Nested AE/comp.aep",
        "Mixed Suite/edit.prproj",
    ]
    assert any(
        record.relative_path == "Mixed Suite/Autosaves/edit_autosave.prproj"
        for record in mixed_db_files
    )
    assert any(record.relative_path == "Mixed Suite/Cache/render.tmp" for record in mixed_db_files)

    resolve_dir = synced_dirs["Resolve Only"]
    resolve_file = files_repo.get_by_dir_and_path(
        project_dir_id=resolve_dir.id,
        relative_path="Resolve Only/color.drp",
    )
    assert resolve_file is not None
    assert resolve_file.is_missing is False

    shutil.rmtree(root_path / "Resolve Only")

    second_scan_result = scan_root_structure(
        root_path=root_path,
        allowed_extensions=allowed_extensions,
    )
    second_sync_result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=second_scan_result,
        synced_at="2026-03-14T11:00:00+00:00",
    )

    refreshed_dirs = {record.relative_path: record for record in dirs_repo.list_by_root(root.id)}
    refreshed_resolve_dir = refreshed_dirs["Resolve Only"]
    refreshed_resolve_file = files_repo.get_by_id(resolve_file.id)

    assert [project_dir.relative_path for project_dir in second_scan_result.project_dirs] == [
        "Mixed Suite",
        "Neutral Exchange",
        "Premiere Only",
    ]
    assert second_sync_result.marked_missing_project_dir_count == 1
    assert second_sync_result.marked_missing_project_file_count == 1
    assert refreshed_resolve_dir.is_missing is True
    assert refreshed_resolve_dir.last_seen_at == "2026-03-14T11:00:00+00:00"
    assert refreshed_resolve_file is not None
    assert refreshed_resolve_file.is_missing is True
    assert refreshed_resolve_file.last_seen_at == "2026-03-14T11:00:00+00:00"


def _materialize_tree_manifest(manifest_path: Path, root_path: Path) -> None:
    root_path.mkdir(parents=True, exist_ok=True)

    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        entry_type, relative_path = line.split(" ", 1)
        target_path = root_path / relative_path
        if entry_type == "d":
            target_path.mkdir(parents=True, exist_ok=True)
            continue
        if entry_type == "f":
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text("sample fixture\n", encoding="utf-8")
            continue
        raise AssertionError(f"Unsupported manifest entry: {line}")
