from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.adapters.filesystem.file_finder import FoundFileInfo
from backup_projects.adapters.filesystem.stat_reader import StatInfo
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.project_dir_scan_service import scan_and_sync_project_dir


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "project-dir-scan.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


@pytest.mark.parametrize(
    ("project_dir_record", "root_record", "expected_exception", "expected_message"),
    [
        (None, None, LookupError, r"Project dir record 11 not found"),
        (
            SimpleNamespace(id=11, root_id=21, relative_path="Show A", is_missing=True),
            None,
            ValueError,
            r"Project dir 11 is marked missing",
        ),
        (
            SimpleNamespace(id=11, root_id=21, relative_path="Show A", is_missing=False),
            None,
            LookupError,
            r"Root record 21 not found",
        ),
        (
            SimpleNamespace(id=11, root_id=21, relative_path="Show A", is_missing=False),
            SimpleNamespace(id=21, path="/tmp/root", is_missing=True),
            ValueError,
            r"Root 21 is marked missing",
        ),
    ],
)
def test_scan_and_sync_project_dir_validates_state_before_scanning(
    monkeypatch,
    project_dir_record,
    root_record,
    expected_exception,
    expected_message: str,
) -> None:
    from backup_projects.services import project_dir_scan_service as scan_service_module

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, project_dir_id):
            return project_dir_record

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

    class FakeProjectFilesRepository:
        def __init__(self, session) -> None:
            return None

    monkeypatch.setattr(
        scan_service_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )
    monkeypatch.setattr(
        scan_service_module,
        "RootsRepository",
        FakeRootsRepository,
    )
    monkeypatch.setattr(
        scan_service_module,
        "ProjectFilesRepository",
        FakeProjectFilesRepository,
    )

    with pytest.raises(expected_exception, match=expected_message):
        scan_service_module.scan_and_sync_project_dir(
            session="fake-session",
            project_dir_id=11,
            scanned_at="2026-03-14T10:00:00+00:00",
        )


def test_scan_and_sync_project_dir_raises_when_target_path_is_not_directory(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)

    root_path = tmp_path / "root"
    root_path.mkdir()
    (root_path / "plain.txt").write_text("not a dir\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix())
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="plain.txt",
        name="plain.txt",
        dir_type="unknown",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    with pytest.raises(NotADirectoryError, match="plain.txt"):
        scan_and_sync_project_dir(
            session=db_session,
            project_dir_id=project_dir.id,
            scanned_at="2026-03-14T10:00:00+00:00",
        )


def test_scan_and_sync_project_dir_classifies_files_and_updates_db(
    db_session: Session,
    tmp_path: Path,
    monkeypatch,
) -> None:
    from backup_projects.services import project_dir_scan_service as scan_service_module

    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    root = _create_root(roots_repo, path=root_path.resolve().as_posix())
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Project A",
        name="Project A",
        dir_type="unknown",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )

    changed_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Project A/changed.txt",
        filename="changed.txt",
        extension="txt",
        size_bytes=10,
        mtime_ns=11,
        ctime_ns=12,
        inode=13,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    reactivated_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Project A/reactivated.txt",
        filename="reactivated.txt",
        extension="txt",
        size_bytes=20,
        mtime_ns=21,
        ctime_ns=22,
        inode=23,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
        is_missing=True,
    )
    unchanged_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Project A/unchanged.txt",
        filename="unchanged.txt",
        extension="txt",
        size_bytes=30,
        mtime_ns=31,
        ctime_ns=32,
        inode=33,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    missing_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Project A/missing.txt",
        filename="missing.txt",
        extension="txt",
        size_bytes=40,
        mtime_ns=41,
        ctime_ns=42,
        inode=43,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )

    project_dir_path = (root_path / "Project A").resolve()
    changed_path = project_dir_path / "changed.txt"
    reactivated_path = project_dir_path / "reactivated.txt"
    unchanged_path = project_dir_path / "unchanged.txt"
    new_path = project_dir_path / "Nested" / "new.txt"

    target_stat = _stat_info(path=project_dir_path, is_dir=True, inode=100)
    stat_map = {
        project_dir_path.as_posix(): target_stat,
        changed_path.as_posix(): _stat_info(
            path=changed_path,
            size_bytes=110,
            mtime_ns=111,
            ctime_ns=112,
            inode=113,
        ),
        reactivated_path.as_posix(): _stat_info(
            path=reactivated_path,
            size_bytes=210,
            mtime_ns=211,
            ctime_ns=212,
            inode=213,
        ),
        unchanged_path.as_posix(): _stat_info(
            path=unchanged_path,
            size_bytes=30,
            mtime_ns=31,
            ctime_ns=32,
            inode=33,
        ),
        new_path.as_posix(): _stat_info(
            path=new_path,
            size_bytes=310,
            mtime_ns=311,
            ctime_ns=312,
            inode=313,
        ),
    }

    def fake_read_stat(path, *, follow_symlinks=False):
        return stat_map.get(Path(path).as_posix())

    def fake_find_files(
        start_path,
        *,
        allowed_extensions,
        excluded_path_patterns=(),
        stay_on_filesystem=False,
        follow_symlinks=False,
        include_hidden=True,
    ):
        assert start_path == project_dir_path.as_posix()
        assert allowed_extensions is None
        assert stay_on_filesystem is True
        assert follow_symlinks is False
        return (
            FoundFileInfo(path=changed_path, relative_path=Path("changed.txt")),
            FoundFileInfo(path=reactivated_path, relative_path=Path("reactivated.txt")),
            FoundFileInfo(path=unchanged_path, relative_path=Path("unchanged.txt")),
            FoundFileInfo(path=new_path, relative_path=Path("Nested/new.txt")),
        )

    monkeypatch.setattr(scan_service_module, "read_stat", fake_read_stat)
    monkeypatch.setattr(scan_service_module, "find_files", fake_find_files)

    result = scan_service_module.scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T10:00:00+00:00",
    )

    refreshed_changed = files_repo.get_by_id(changed_file.id)
    refreshed_reactivated = files_repo.get_by_id(reactivated_file.id)
    refreshed_unchanged = files_repo.get_by_id(unchanged_file.id)
    refreshed_missing = files_repo.get_by_id(missing_file.id)
    created_new = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="Project A/Nested/new.txt",
    )

    assert result.project_dir_present is True
    assert result.project_dir_path == project_dir_path.as_posix()
    assert result.scanned_file_count == 4
    assert result.new_file_count == 1
    assert result.changed_file_count == 1
    assert result.reactivated_file_count == 1
    assert result.missing_file_count == 1
    assert result.unchanged_file_count == 1
    assert refreshed_changed is not None
    assert refreshed_changed.size_bytes == 110
    assert refreshed_changed.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_changed.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_reactivated is not None
    assert refreshed_reactivated.is_missing is False
    assert refreshed_reactivated.size_bytes == 210
    assert refreshed_reactivated.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_reactivated.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_unchanged is not None
    assert refreshed_unchanged.size_bytes == 30
    assert refreshed_unchanged.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_unchanged.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_missing is not None
    assert refreshed_missing.is_missing is True
    assert refreshed_missing.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_missing.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert created_new is not None
    assert created_new.relative_path == "Project A/Nested/new.txt"
    assert created_new.first_seen_at == "2026-03-14T10:00:00+00:00"
    assert created_new.last_seen_at == "2026-03-14T10:00:00+00:00"


def test_scan_and_sync_project_dir_supports_root_relative_path_empty(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root-as-project"
    root_path.mkdir()
    (root_path / "top.txt").write_text("top-level\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix())
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="",
        name="root-as-project",
        dir_type="unknown",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    result = scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T10:00:00+00:00",
    )

    created_file = files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="top.txt",
    )

    assert result.project_dir_path == root_path.resolve().as_posix()
    assert result.project_dir_present is True
    assert result.scanned_file_count == 1
    assert result.new_file_count == 1
    assert created_file is not None
    assert created_file.relative_path == "top.txt"


def test_scan_and_sync_project_dir_marks_all_files_missing_when_directory_absent(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    root_path.mkdir()

    root = _create_root(roots_repo, path=root_path.resolve().as_posix())
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Missing Dir",
        name="Missing Dir",
        dir_type="unknown",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )
    file_one = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Missing Dir/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=10,
        mtime_ns=11,
        ctime_ns=12,
        inode=13,
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )
    file_two = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="Missing Dir/media.mov",
        filename="media.mov",
        extension="mov",
        size_bytes=20,
        mtime_ns=21,
        ctime_ns=22,
        inode=23,
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    result = scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T10:00:00+00:00",
    )

    refreshed_project_dir = dirs_repo.get_by_id(project_dir.id)
    refreshed_file_one = files_repo.get_by_id(file_one.id)
    refreshed_file_two = files_repo.get_by_id(file_two.id)

    assert result.project_dir_present is False
    assert result.scanned_file_count == 0
    assert result.missing_file_count == 2
    assert refreshed_project_dir is not None
    assert refreshed_project_dir.is_missing is False
    assert refreshed_file_one is not None
    assert refreshed_file_one.is_missing is True
    assert refreshed_file_one.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert refreshed_file_two is not None
    assert refreshed_file_two.is_missing is True
    assert refreshed_file_two.last_seen_at == "2026-03-14T10:00:00+00:00"


def test_scan_and_sync_project_dir_skips_disappeared_and_incomplete_stat_files(
    db_session: Session,
    tmp_path: Path,
    monkeypatch,
) -> None:
    from backup_projects.services import project_dir_scan_service as scan_service_module

    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    root = _create_root(roots_repo, path=root_path.resolve().as_posix())
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Project A",
        name="Project A",
        dir_type="unknown",
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
    )

    project_dir_path = (root_path / "Project A").resolve()
    vanished_path = project_dir_path / "vanished.txt"
    partial_path = project_dir_path / "partial.txt"

    target_stat = _stat_info(path=project_dir_path, is_dir=True, inode=100)
    partial_stat = StatInfo(
        path=partial_path,
        exists=True,
        is_file=True,
        is_dir=False,
        is_symlink=False,
        size_bytes=None,
        mtime_ns=123,
        ctime_ns=124,
        inode=125,
        device_id=126,
    )
    stat_map = {
        project_dir_path.as_posix(): target_stat,
        partial_path.as_posix(): partial_stat,
    }

    def fake_read_stat(path, *, follow_symlinks=False):
        return stat_map.get(Path(path).as_posix())

    def fake_find_files(
        start_path,
        *,
        allowed_extensions,
        excluded_path_patterns=(),
        stay_on_filesystem=False,
        follow_symlinks=False,
        include_hidden=True,
    ):
        return (
            FoundFileInfo(path=vanished_path, relative_path=Path("vanished.txt")),
            FoundFileInfo(path=partial_path, relative_path=Path("partial.txt")),
        )

    monkeypatch.setattr(scan_service_module, "read_stat", fake_read_stat)
    monkeypatch.setattr(scan_service_module, "find_files", fake_find_files)

    result = scan_service_module.scan_and_sync_project_dir(
        session=db_session,
        project_dir_id=project_dir.id,
        scanned_at="2026-03-14T10:00:00+00:00",
    )

    assert result.scanned_file_count == 0
    assert result.new_file_count == 0
    assert files_repo.list_by_project_dir(project_dir.id) == []


def _create_root(roots_repo: RootsRepository, *, path: str, is_missing: bool = False):
    return roots_repo.create(
        raid_name="raid_sample",
        name=Path(path).name or "root",
        path=path,
        device_id=101,
        inode=202,
        mtime_ns=303,
        ctime_ns=404,
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
        is_missing=is_missing,
    )


def _stat_info(
    *,
    path: Path,
    is_dir: bool = False,
    size_bytes: int | None = 1,
    mtime_ns: int = 2,
    ctime_ns: int = 3,
    inode: int | None = 4,
    device_id: int | None = 5,
) -> StatInfo:
    return StatInfo(
        path=path,
        exists=True,
        is_file=not is_dir,
        is_dir=is_dir,
        is_symlink=False,
        size_bytes=None if is_dir else size_bytes,
        mtime_ns=mtime_ns,
        ctime_ns=ctime_ns,
        inode=inode,
        device_id=device_id,
    )
