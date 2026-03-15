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
from backup_projects.domain.enums import IncludePathType
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.manual_include_scan_service import apply_manual_includes_for_root

FIXTURE_PATH = Path("tests/fixtures/manual_includes/sample_tree_manifest.txt")


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "manual-include-sample.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_manual_include_scan_on_sanitized_sample_tree(
    db_session: Session,
    tmp_path: Path,
) -> None:
    root_path = tmp_path / "sample_root"
    _materialize_tree_manifest(FIXTURE_PATH, root_path)

    roots_repo = RootsRepository(db_session)
    includes_repo = ManualIncludesRepository(db_session)
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
        relative_path="workspace/ProjectAlpha",
        name="ProjectAlpha",
        dir_type="premiere",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )
    unrelated_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="unrelated",
        name="unrelated",
        dir_type="unknown",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    _seed_project_file(
        files_repo=files_repo,
        project_dir_id=project_dir.id,
        file_path=root_path / "workspace" / "ProjectAlpha" / "unchanged.txt",
        stored_relative_path="workspace/ProjectAlpha/unchanged.txt",
        first_seen_at="2026-03-14T09:30:00+00:00",
    )
    files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/changed.txt",
        filename="changed.txt",
        extension="txt",
        size_bytes=1,
        mtime_ns=2,
        ctime_ns=3,
        inode=4,
        first_seen_at="2026-03-14T09:30:00+00:00",
        last_seen_at="2026-03-14T09:30:00+00:00",
    )
    files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/reactivated.txt",
        filename="reactivated.txt",
        extension="txt",
        size_bytes=5,
        mtime_ns=6,
        ctime_ns=7,
        inode=8,
        first_seen_at="2026-03-14T09:30:00+00:00",
        last_seen_at="2026-03-14T09:30:00+00:00",
        is_missing=True,
    )
    _seed_project_file(
        files_repo=files_repo,
        project_dir_id=unrelated_dir.id,
        file_path=root_path / "unrelated" / "untouched.txt",
        stored_relative_path="unrelated/untouched.txt",
        first_seen_at="2026-03-14T09:30:00+00:00",
    )

    includes_repo.create(
        root_id=root.id,
        relative_path="incoming/direct_file.txt",
        include_path_type=IncludePathType.FILE.value,
        recursive=False,
        force_include=True,
        enabled=True,
        created_at="2026-03-14T09:45:00+00:00",
        updated_at="2026-03-14T09:45:00+00:00",
    )
    includes_repo.create(
        root_id=root.id,
        relative_path="packages",
        include_path_type=IncludePathType.DIRECTORY.value,
        recursive=False,
        force_include=False,
        enabled=True,
        created_at="2026-03-14T09:45:00+00:00",
        updated_at="2026-03-14T09:45:00+00:00",
    )
    includes_repo.create(
        root_id=root.id,
        relative_path="workspace/ProjectAlpha",
        include_path_type=IncludePathType.DIRECTORY.value,
        recursive=True,
        force_include=False,
        enabled=True,
        created_at="2026-03-14T09:45:00+00:00",
        updated_at="2026-03-14T09:45:00+00:00",
    )
    includes_repo.create(
        root_id=root.id,
        relative_path="disabled/ignored.txt",
        include_path_type=IncludePathType.FILE.value,
        recursive=False,
        force_include=False,
        enabled=False,
        created_at="2026-03-14T09:45:00+00:00",
        updated_at="2026-03-14T09:45:00+00:00",
    )

    result = apply_manual_includes_for_root(
        session=db_session,
        root_id=root.id,
        applied_at="2026-03-14T10:00:00+00:00",
    )

    items_by_path = {item.relative_path: item for item in result.item_results}
    incoming_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="incoming")
    packages_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="packages")
    nested_anchor = dirs_repo.get_by_root_and_path(
        root_id=root.id,
        relative_path="workspace/ProjectAlpha/Nested",
    )
    direct_file = None
    packages_top = None
    if incoming_anchor is not None:
        direct_file = files_repo.get_by_dir_and_path(
            project_dir_id=incoming_anchor.id,
            relative_path="incoming/direct_file.txt",
        )
    if packages_anchor is not None:
        packages_top = files_repo.get_by_dir_and_path(
            project_dir_id=packages_anchor.id,
            relative_path="packages/top.txt",
        )
        packages_deep = files_repo.get_by_dir_and_path(
            project_dir_id=packages_anchor.id,
            relative_path="packages/Sub/deep.txt",
        )
    else:
        packages_deep = None
    changed_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/changed.txt",
    )
    reactivated_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/reactivated.txt",
    )
    unchanged_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/unchanged.txt",
    )
    new_nested_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="workspace/ProjectAlpha/Nested/new_nested.txt",
    )
    disabled_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="disabled")
    unrelated_file = files_repo.get_by_dir_and_path(
        project_dir_id=unrelated_dir.id,
        relative_path="unrelated/untouched.txt",
    )

    assert result.processed_include_count == 4
    assert result.applied_include_count == 3
    assert result.skipped_disabled_include_count == 1
    assert result.created_project_dir_count == 2
    assert result.updated_project_dir_count == 1
    assert result.created_project_file_count == 3
    assert result.updated_project_file_count == 1
    assert result.reactivated_project_file_count == 1
    assert result.unchanged_project_file_count == 1
    assert result.skipped_file_count == 1
    assert result.matched_file_count == 6

    assert items_by_path["incoming/direct_file.txt"].force_include is True
    assert items_by_path["packages"].skipped_file_count == 1
    assert items_by_path["disabled/ignored.txt"].status.value == "skipped_disabled"

    assert incoming_anchor is not None
    assert packages_anchor is not None
    assert nested_anchor is None
    assert direct_file is not None
    assert packages_top is not None
    assert packages_deep is None
    assert changed_file is not None
    changed_path = root_path / "workspace" / "ProjectAlpha" / "changed.txt"
    assert changed_file.size_bytes == changed_path.stat().st_size
    assert reactivated_file is not None
    assert reactivated_file.is_missing is False
    assert unchanged_file is not None
    assert unchanged_file.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert new_nested_file is not None
    assert disabled_anchor is None
    assert unrelated_file is not None
    assert unrelated_file.is_missing is False
    assert unrelated_file.last_seen_at == "2026-03-14T09:30:00+00:00"


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


def _seed_project_file(
    *,
    files_repo: ProjectFilesRepository,
    project_dir_id: int,
    file_path: Path,
    stored_relative_path: str,
    first_seen_at: str,
) -> None:
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
