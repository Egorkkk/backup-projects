from __future__ import annotations

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
from backup_projects.services.project_dir_scan_service import scan_and_sync_project_dir

FIXTURE_PATH = Path("tests/fixtures/incremental_scan/sample_tree_manifest.txt")


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "incremental-sample.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_incremental_scan_rescan_on_sanitized_sample_tree(
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
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="workspace/Project Alpha",
        name="Project Alpha",
        dir_type="premiere",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )
    _seed_project_files_from_disk(
        files_repo=files_repo,
        project_dir_id=project_dir.id,
        project_dir_relative_path=project_dir.relative_path,
        project_dir_path=root_path / "workspace" / "Project Alpha",
        first_seen_at="2026-03-14T09:30:00+00:00",
    )

    first_result = scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T10:00:00+00:00",
    )

    assert first_result.project_dir_present is True
    assert first_result.scanned_file_count == 3
    assert first_result.new_file_count == 0
    assert first_result.changed_file_count == 0
    assert first_result.reactivated_file_count == 0
    assert first_result.missing_file_count == 0
    assert first_result.unchanged_file_count == 3

    project_dir_path = root_path / "workspace" / "Project Alpha"
    edit_path = project_dir_path / "edit.prproj"
    notes_path = project_dir_path / "notes.txt"
    new_path = project_dir_path / "new.aaf"

    edit_path.write_text("edited fixture with larger content\n", encoding="utf-8")
    notes_path.unlink()
    new_path.write_text("new timeline\n", encoding="utf-8")

    second_result = scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T11:00:00+00:00",
    )

    changed_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/Project Alpha/edit.prproj",
    )
    missing_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/Project Alpha/notes.txt",
    )
    new_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/Project Alpha/new.aaf",
    )
    unchanged_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/Project Alpha/Sub/clip.mov",
    )

    assert second_result.project_dir_present is True
    assert second_result.scanned_file_count == 3
    assert second_result.new_file_count == 1
    assert second_result.changed_file_count == 1
    assert second_result.reactivated_file_count == 0
    assert second_result.missing_file_count == 1
    assert second_result.unchanged_file_count == 1
    assert changed_file is not None
    assert changed_file.last_seen_at == "2026-03-14T11:00:00+00:00"
    assert changed_file.size_bytes == edit_path.stat().st_size
    assert missing_file is not None
    assert missing_file.is_missing is True
    assert missing_file.last_seen_at == "2026-03-14T11:00:00+00:00"
    assert new_file is not None
    assert new_file.is_missing is False
    assert new_file.first_seen_at == "2026-03-14T11:00:00+00:00"
    assert new_file.last_seen_at == "2026-03-14T11:00:00+00:00"
    assert unchanged_file is not None
    assert unchanged_file.is_missing is False
    assert unchanged_file.last_seen_at == "2026-03-14T11:00:00+00:00"

    notes_path.write_text("reactivated notes\n", encoding="utf-8")

    third_result = scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T12:00:00+00:00",
    )

    reactivated_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/Project Alpha/notes.txt",
    )

    assert third_result.project_dir_present is True
    assert third_result.scanned_file_count == 4
    assert third_result.new_file_count == 0
    assert third_result.changed_file_count == 0
    assert third_result.reactivated_file_count == 1
    assert third_result.missing_file_count == 0
    assert third_result.unchanged_file_count == 3
    assert reactivated_file is not None
    assert reactivated_file.is_missing is False
    assert reactivated_file.first_seen_at == "2026-03-14T09:30:00+00:00"
    assert reactivated_file.last_seen_at == "2026-03-14T12:00:00+00:00"


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


def _seed_project_files_from_disk(
    *,
    files_repo: ProjectFilesRepository,
    project_dir_id: int,
    project_dir_relative_path: str,
    project_dir_path: Path,
    first_seen_at: str,
) -> None:
    for file_path in sorted(path for path in project_dir_path.rglob("*") if path.is_file()):
        relative_inside_project_dir = file_path.relative_to(project_dir_path).as_posix()
        stored_relative_path = (
            relative_inside_project_dir
            if project_dir_relative_path == ""
            else f"{project_dir_relative_path}/{relative_inside_project_dir}"
        )
        stat_result = file_path.stat()
        files_repo.create(
            project_dir_id=project_dir_id,
            relative_path=stored_relative_path,
            filename=file_path.name,
            extension=file_path.suffix.lower().lstrip("."),
            size_bytes=stat_result.st_size,
            mtime_ns=stat_result.st_mtime_ns,
            ctime_ns=stat_result.st_ctime_ns,
            inode=stat_result.st_ino,
            first_seen_at=first_seen_at,
            last_seen_at=first_seen_at,
        )
