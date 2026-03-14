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
from backup_projects.services.structural_scan_service import (
    ScannedProjectDir,
    ScannedProjectFile,
    StructuralScanResult,
)
from backup_projects.services.structural_scan_sync_service import (
    StructuralScanSyncResult,
    sync_structural_scan_result,
)


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "structural-sync.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_sync_structural_scan_result_creates_new_dirs_and_files(db_session: Session) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")

    result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=_make_scan_result(
            root_path=root.path,
            project_dirs=(
                _scanned_dir(
                    relative_path="Show A",
                    name="Show A",
                    dir_type="premiere",
                    files=(
                        _scanned_file(relative_path="Assets/clip.mov", filename="clip.mov"),
                        _scanned_file(relative_path="edit.prproj", filename="edit.prproj"),
                    ),
                ),
            ),
        ),
        synced_at="2026-03-14T10:00:00+00:00",
    )

    created_dir = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="Show A")
    created_file = files_repo.get_by_dir_and_path(
        project_dir_id=created_dir.id,
        relative_path="Show A/edit.prproj",
    )
    nested_file = files_repo.get_by_dir_and_path(
        project_dir_id=created_dir.id,
        relative_path="Show A/Assets/clip.mov",
    )

    assert result == StructuralScanSyncResult(
        root_id=root.id,
        root_path=root.path,
        synced_at="2026-03-14T10:00:00+00:00",
        scanned_project_dir_count=1,
        created_project_dir_count=1,
        updated_project_dir_count=0,
        reactivated_project_dir_count=0,
        marked_missing_project_dir_count=0,
        scanned_project_file_count=2,
        created_project_file_count=2,
        updated_project_file_count=0,
        reactivated_project_file_count=0,
        marked_missing_project_file_count=0,
    )
    assert created_dir is not None
    assert created_dir.name == "Show A"
    assert created_dir.dir_type == "premiere"
    assert created_dir.is_missing is False
    assert created_dir.first_seen_at == "2026-03-14T10:00:00+00:00"
    assert created_file is not None
    assert nested_file is not None
    assert created_file.relative_path == "Show A/edit.prproj"
    assert nested_file.relative_path == "Show A/Assets/clip.mov"
    assert created_file.first_seen_at == "2026-03-14T10:00:00+00:00"


def test_sync_structural_scan_result_updates_existing_dirs_and_files(db_session: Session) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Show A",
        name="Show A",
        dir_type="premiere",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )
    project_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Show A/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=10,
        mtime_ns=11,
        ctime_ns=12,
        inode=13,
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=_make_scan_result(
            root_path=root.path,
            project_dirs=(
                _scanned_dir(
                    relative_path="Show A",
                    name="Show A",
                    dir_type="mixed",
                    files=(
                        _scanned_file(
                            relative_path="edit.prproj",
                            filename="edit.prproj",
                            size_bytes=100,
                            mtime_ns=101,
                            ctime_ns=102,
                            inode=103,
                        ),
                    ),
                ),
            ),
        ),
        synced_at="2026-03-14T10:00:00+00:00",
    )

    refreshed_dir = dirs_repo.get_by_id(project_dir.id)
    refreshed_file = files_repo.get_by_id(project_file.id)

    assert result.created_project_dir_count == 0
    assert result.updated_project_dir_count == 1
    assert result.reactivated_project_dir_count == 0
    assert result.created_project_file_count == 0
    assert result.updated_project_file_count == 1
    assert result.reactivated_project_file_count == 0
    assert refreshed_dir is not None
    assert refreshed_dir.dir_type == "mixed"
    assert refreshed_dir.first_seen_at == "2026-03-14T09:00:00+00:00"
    assert refreshed_dir.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_file is not None
    assert refreshed_file.size_bytes == 100
    assert refreshed_file.mtime_ns == 101
    assert refreshed_file.ctime_ns == 102
    assert refreshed_file.inode == 103
    assert refreshed_file.first_seen_at == "2026-03-14T09:00:00+00:00"
    assert refreshed_file.last_seen_at == "2026-03-14T10:00:00+00:00"


def test_sync_structural_scan_result_reactivates_missing_dirs_and_files(
    db_session: Session,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Show A",
        name="Show A",
        dir_type="premiere",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
        is_missing=True,
    )
    project_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Show A/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=10,
        mtime_ns=11,
        ctime_ns=12,
        inode=13,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
        is_missing=True,
    )

    result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=_make_scan_result(
            root_path=root.path,
            project_dirs=(
                _scanned_dir(
                    relative_path="Show A",
                    name="Show A",
                    dir_type="mixed",
                    files=(
                        _scanned_file(
                            relative_path="edit.prproj",
                            filename="edit.prproj",
                            size_bytes=100,
                            mtime_ns=101,
                            ctime_ns=102,
                            inode=103,
                        ),
                    ),
                ),
            ),
        ),
        synced_at="2026-03-14T10:00:00+00:00",
    )

    refreshed_dir = dirs_repo.get_by_id(project_dir.id)
    refreshed_file = files_repo.get_by_id(project_file.id)

    assert result.reactivated_project_dir_count == 1
    assert result.updated_project_dir_count == 0
    assert result.reactivated_project_file_count == 1
    assert result.updated_project_file_count == 0
    assert refreshed_dir is not None
    assert refreshed_dir.is_missing is False
    assert refreshed_dir.dir_type == "mixed"
    assert refreshed_dir.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_dir.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_file is not None
    assert refreshed_file.is_missing is False
    assert refreshed_file.size_bytes == 100
    assert refreshed_file.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_file.last_seen_at == "2026-03-14T10:00:00+00:00"


def test_sync_structural_scan_result_marks_missing_dirs_and_transitively_marks_files(
    db_session: Session,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")
    kept_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Keep",
        name="Keep",
        dir_type="premiere",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    kept_file = files_repo.create(
        project_dir_id=kept_dir.id,
        relative_path="Keep/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=10,
        mtime_ns=11,
        ctime_ns=12,
        inode=13,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    untouched_file = files_repo.create(
        project_dir_id=kept_dir.id,
        relative_path="Keep/notes.txt",
        filename="notes.txt",
        extension="txt",
        size_bytes=20,
        mtime_ns=21,
        ctime_ns=22,
        inode=23,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    missing_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Gone",
        name="Gone",
        dir_type="premiere",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    missing_file = files_repo.create(
        project_dir_id=missing_dir.id,
        relative_path="Gone/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=30,
        mtime_ns=31,
        ctime_ns=32,
        inode=33,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )

    result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=_make_scan_result(
            root_path=root.path,
            project_dirs=(
                _scanned_dir(
                    relative_path="Keep",
                    name="Keep",
                    dir_type="mixed",
                    files=(
                        _scanned_file(
                            relative_path="edit.prproj",
                            filename="edit.prproj",
                            size_bytes=100,
                            mtime_ns=101,
                            ctime_ns=102,
                            inode=103,
                        ),
                    ),
                ),
            ),
        ),
        synced_at="2026-03-14T10:00:00+00:00",
    )

    refreshed_kept_dir = dirs_repo.get_by_id(kept_dir.id)
    refreshed_missing_dir = dirs_repo.get_by_id(missing_dir.id)
    refreshed_kept_file = files_repo.get_by_id(kept_file.id)
    refreshed_untouched_file = files_repo.get_by_id(untouched_file.id)
    refreshed_missing_file = files_repo.get_by_id(missing_file.id)

    assert result.updated_project_dir_count == 1
    assert result.marked_missing_project_dir_count == 1
    assert result.updated_project_file_count == 1
    assert result.marked_missing_project_file_count == 1
    assert refreshed_kept_dir is not None
    assert refreshed_kept_dir.is_missing is False
    assert refreshed_kept_dir.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_missing_dir is not None
    assert refreshed_missing_dir.is_missing is True
    assert refreshed_missing_dir.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_kept_file is not None
    assert refreshed_kept_file.is_missing is False
    assert refreshed_kept_file.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_untouched_file is not None
    assert refreshed_untouched_file.is_missing is False
    assert refreshed_untouched_file.last_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_missing_file is not None
    assert refreshed_missing_file.is_missing is True
    assert refreshed_missing_file.last_seen_at == "2026-03-14T10:00:00+00:00"


def test_sync_structural_scan_result_raises_lookup_error_for_unknown_root(
    db_session: Session,
) -> None:
    with pytest.raises(LookupError):
        sync_structural_scan_result(
            session=db_session,
            root_id=9999,
            scan_result=_make_scan_result(root_path="/missing/root", project_dirs=()),
            synced_at="2026-03-14T10:00:00+00:00",
        )


def test_sync_structural_scan_result_rejects_root_path_mismatch(db_session: Session) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")

    with pytest.raises(ValueError, match="root_path"):
        sync_structural_scan_result(
            session=db_session,
            root_id=root.id,
            scan_result=_make_scan_result(root_path="/mnt/raid_a/other-root", project_dirs=()),
            synced_at="2026-03-14T10:00:00+00:00",
        )

    assert dirs_repo.list_by_root(root.id) == []


def test_sync_structural_scan_result_supports_root_itself_as_project_dir(
    db_session: Session,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")

    result = sync_structural_scan_result(
        session=db_session,
        root_id=root.id,
        scan_result=_make_scan_result(
            root_path=root.path,
            project_dirs=(
                _scanned_dir(
                    relative_path="",
                    name="show-root",
                    dir_type="premiere",
                    files=(
                        _scanned_file(relative_path="media.mov", filename="media.mov"),
                    ),
                ),
            ),
        ),
        synced_at="2026-03-14T10:00:00+00:00",
    )

    project_dir = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="")
    project_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="media.mov",
    )

    assert result.created_project_dir_count == 1
    assert result.created_project_file_count == 1
    assert project_dir is not None
    assert project_file is not None
    assert project_file.relative_path == "media.mov"


def test_sync_structural_scan_result_rejects_duplicate_project_dir_identities(
    db_session: Session,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")

    with pytest.raises(ValueError, match="Duplicate project_dir identity"):
        sync_structural_scan_result(
            session=db_session,
            root_id=root.id,
            scan_result=_make_scan_result(
                root_path=root.path,
                project_dirs=(
                    _scanned_dir(relative_path="Show A", name="Show A", dir_type="premiere"),
                    _scanned_dir(relative_path="Show A", name="Show A", dir_type="mixed"),
                ),
            ),
            synced_at="2026-03-14T10:00:00+00:00",
        )

    assert dirs_repo.list_by_root(root.id) == []


def test_sync_structural_scan_result_rejects_duplicate_project_file_identities(
    db_session: Session,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    root = _create_root(roots_repo, path="/mnt/raid_a/show-root")

    with pytest.raises(ValueError, match="Duplicate project_file identity"):
        sync_structural_scan_result(
            session=db_session,
            root_id=root.id,
            scan_result=_make_scan_result(
                root_path=root.path,
                project_dirs=(
                    _scanned_dir(
                        relative_path="Show A",
                        name="Show A",
                        dir_type="premiere",
                        files=(
                            _scanned_file(relative_path="edit.prproj", filename="edit.prproj"),
                            _scanned_file(relative_path="edit.prproj", filename="edit.prproj"),
                        ),
                    ),
                ),
            ),
            synced_at="2026-03-14T10:00:00+00:00",
        )

    assert dirs_repo.list_by_root(root.id) == []


def _create_root(roots_repo: RootsRepository, *, path: str):
    return roots_repo.create(
        raid_name="raid_a",
        name=path.rsplit("/", 1)[-1],
        path=path,
        device_id=100,
        inode=200,
        mtime_ns=300,
        ctime_ns=400,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )


def _make_scan_result(
    *,
    root_path: str,
    project_dirs: tuple[ScannedProjectDir, ...],
) -> StructuralScanResult:
    return StructuralScanResult(root_path=root_path, project_dirs=project_dirs)


def _scanned_dir(
    *,
    relative_path: str,
    name: str,
    dir_type: str,
    files: tuple[ScannedProjectFile, ...] = (),
) -> ScannedProjectDir:
    return ScannedProjectDir(
        relative_path=relative_path,
        name=name,
        dir_type=dir_type,
        files=files,
    )


def _scanned_file(
    *,
    relative_path: str,
    filename: str,
    extension: str | None = None,
    size_bytes: int = 1,
    mtime_ns: int = 2,
    ctime_ns: int = 3,
    inode: int | None = 4,
) -> ScannedProjectFile:
    return ScannedProjectFile(
        relative_path=relative_path,
        filename=filename,
        extension=extension or filename.rsplit(".", 1)[-1],
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
        ctime_ns=ctime_ns,
        inode=inode,
    )
