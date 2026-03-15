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
from backup_projects.adapters.filesystem.stat_reader import read_stat
from backup_projects.domain.enums import IncludePathType
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.manual_include_scan_service import (
    ManualIncludeApplyStatus,
    apply_manual_includes_for_root,
)


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "manual-includes.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_apply_manual_includes_for_root_raises_for_unknown_root(db_session: Session) -> None:
    with pytest.raises(LookupError, match=r"Root record 999 not found"):
        apply_manual_includes_for_root(
            session=db_session,
            root_id=999,
            applied_at="2026-03-14T10:00:00+00:00",
        )


def test_apply_manual_includes_for_root_raises_for_missing_root(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    root_path = tmp_path / "root"
    root_path.mkdir()
    root = _create_root(
        roots_repo,
        path=root_path.resolve().as_posix(),
        is_missing=True,
    )

    with pytest.raises(ValueError, match=rf"Root {root.id} is marked missing"):
        apply_manual_includes_for_root(
            session=db_session,
            root_id=root.id,
            applied_at="2026-03-14T10:00:00+00:00",
        )


def test_apply_manual_includes_for_root_raises_for_absent_root_path(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    root_path = tmp_path / "missing-root"
    root = _create_root(roots_repo, path=root_path.resolve().as_posix())

    with pytest.raises(FileNotFoundError, match="missing-root"):
        apply_manual_includes_for_root(
            session=db_session,
            root_id=root.id,
            applied_at="2026-03-14T10:00:00+00:00",
        )


def test_apply_manual_includes_for_root_raises_for_non_directory_root_path(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    root_file = tmp_path / "root-file.txt"
    root_file.write_text("not a directory\n", encoding="utf-8")
    root = _create_root(roots_repo, path=root_file.resolve().as_posix())

    with pytest.raises(NotADirectoryError, match="root-file.txt"):
        apply_manual_includes_for_root(
            session=db_session,
            root_id=root.id,
            applied_at="2026-03-14T10:00:00+00:00",
        )


def test_apply_manual_includes_for_root_summarizes_invalid_missing_and_type_mismatch_rows(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    includes_repo = ManualIncludesRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    root_path.mkdir()
    (tmp_path / "outside.txt").write_text("outside\n", encoding="utf-8")
    (root_path / "dir-target").mkdir()
    (root_path / "plain.txt").write_text("plain\n", encoding="utf-8")
    (root_path / "valid.txt").write_text("valid\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix(), name="root")

    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="disabled.txt",
        include_path_type=IncludePathType.FILE.value,
        enabled=False,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="",
        include_path_type=IncludePathType.FILE.value,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="../outside.txt",
        include_path_type=IncludePathType.FILE.value,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="bogus.txt",
        include_path_type="bogus",
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="missing.txt",
        include_path_type=IncludePathType.FILE.value,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="dir-target",
        include_path_type=IncludePathType.FILE.value,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="plain.txt",
        include_path_type=IncludePathType.DIRECTORY.value,
    )
    valid_include = _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="valid.txt",
        include_path_type=IncludePathType.FILE.value,
        force_include=True,
    )

    result = apply_manual_includes_for_root(
        session=db_session,
        root_id=root.id,
        applied_at="2026-03-14T10:00:00+00:00",
    )

    items_by_path = {item.relative_path: item for item in result.item_results}

    assert result.processed_include_count == 8
    assert result.applied_include_count == 1
    assert result.skipped_disabled_include_count == 1
    assert result.invalid_include_count == 3
    assert result.missing_target_include_count == 1
    assert result.type_mismatch_include_count == 2
    assert result.error_include_count == 0
    assert items_by_path["disabled.txt"].status is ManualIncludeApplyStatus.SKIPPED_DISABLED
    assert items_by_path[""].status is ManualIncludeApplyStatus.INVALID_INCLUDE
    assert items_by_path["../outside.txt"].status is ManualIncludeApplyStatus.INVALID_INCLUDE
    assert items_by_path["bogus.txt"].status is ManualIncludeApplyStatus.INVALID_INCLUDE
    assert items_by_path["missing.txt"].status is ManualIncludeApplyStatus.MISSING_TARGET
    assert items_by_path["dir-target"].status is ManualIncludeApplyStatus.TYPE_MISMATCH
    assert items_by_path["plain.txt"].status is ManualIncludeApplyStatus.TYPE_MISMATCH
    assert items_by_path["valid.txt"].status is ManualIncludeApplyStatus.APPLIED
    assert items_by_path["valid.txt"].force_include is True
    assert items_by_path["valid.txt"].resolved_target_path == (
        root_path / "valid.txt"
    ).resolve().as_posix()

    created_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="")
    created_file = None
    if created_anchor is not None:
        created_file = files_repo.get_by_dir_and_path(
            project_dir_id=created_anchor.id,
            relative_path="valid.txt",
        )

    assert created_anchor is not None
    assert created_file is not None
    assert created_file.is_missing is False
    assert created_file.last_seen_at == "2026-03-14T10:00:00+00:00"
    assert dirs_repo.list_by_root(root.id) == [created_anchor]
    assert len(files_repo.list_by_project_dir(created_anchor.id)) == 1
    assert valid_include.force_include is True


def test_apply_manual_includes_for_root_file_include_oserror_becomes_item_error_and_continues(
    db_session: Session,
    tmp_path: Path,
    monkeypatch,
) -> None:
    from backup_projects.services import manual_include_scan_service as service_module

    roots_repo = RootsRepository(db_session)
    includes_repo = ManualIncludesRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    root_path.mkdir()
    (root_path / "a-error.txt").write_text("error\n", encoding="utf-8")
    (root_path / "z-good.txt").write_text("good\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix(), name="root")
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="a-error.txt",
        include_path_type=IncludePathType.FILE.value,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="z-good.txt",
        include_path_type=IncludePathType.FILE.value,
    )

    original_apply = service_module._apply_observed_file_path

    def fake_apply_observed_file_path(*, file_path, **kwargs):
        if Path(file_path).name == "a-error.txt":
            raise OSError("simulated file include failure")
        return original_apply(file_path=file_path, **kwargs)

    monkeypatch.setattr(
        service_module,
        "_apply_observed_file_path",
        fake_apply_observed_file_path,
    )

    result = apply_manual_includes_for_root(
        session=db_session,
        root_id=root.id,
        applied_at="2026-03-14T11:00:00+00:00",
    )

    items_by_path = {item.relative_path: item for item in result.item_results}
    root_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="")
    good_file = None
    if root_anchor is not None:
        good_file = files_repo.get_by_dir_and_path(
            project_dir_id=root_anchor.id,
            relative_path="z-good.txt",
        )

    assert result.processed_include_count == 2
    assert result.applied_include_count == 1
    assert result.error_include_count == 1
    assert items_by_path["a-error.txt"].status is ManualIncludeApplyStatus.ERROR
    assert items_by_path["a-error.txt"].message == "simulated file include failure"
    assert items_by_path["z-good.txt"].status is ManualIncludeApplyStatus.APPLIED
    assert good_file is not None
    assert root_anchor is not None
    assert files_repo.list_by_project_dir(root_anchor.id) == [good_file]


def test_apply_manual_includes_for_root_non_recursive_directory_skips_nested_dirs_and_stat_errors(
    db_session: Session,
    tmp_path: Path,
    monkeypatch,
) -> None:
    from backup_projects.services import manual_include_scan_service as service_module

    roots_repo = RootsRepository(db_session)
    includes_repo = ManualIncludesRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    drop_path = root_path / "Drop"
    nested_path = drop_path / "Nested"
    nested_path.mkdir(parents=True)
    (drop_path / "good.txt").write_text("good\n", encoding="utf-8")
    (drop_path / "bad.txt").write_text("bad\n", encoding="utf-8")
    (nested_path / "inside.txt").write_text("inside\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix(), name="root")
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="Drop",
        include_path_type=IncludePathType.DIRECTORY.value,
        recursive=False,
    )

    original_read_stat = service_module.read_stat

    def fake_read_stat(path, *, follow_symlinks=False):
        if Path(path).name == "bad.txt":
            raise PermissionError("simulated stat error")
        return original_read_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(service_module, "read_stat", fake_read_stat)

    result = apply_manual_includes_for_root(
        session=db_session,
        root_id=root.id,
        applied_at="2026-03-14T12:00:00+00:00",
    )

    item = result.item_results[0]
    drop_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="Drop")
    good_file = None
    if drop_anchor is not None:
        good_file = files_repo.get_by_dir_and_path(
            project_dir_id=drop_anchor.id,
            relative_path="Drop/good.txt",
        )

    assert result.applied_include_count == 1
    assert result.skipped_file_count == 2
    assert item.status is ManualIncludeApplyStatus.APPLIED
    assert item.matched_file_count == 1
    assert item.skipped_file_count == 2
    assert item.created_project_dir_count == 1
    assert item.created_project_file_count == 1
    assert drop_anchor is not None
    assert good_file is not None
    assert files_repo.get_by_dir_and_path(
        project_dir_id=drop_anchor.id,
        relative_path="Drop/Nested/inside.txt",
    ) is None


def test_apply_manual_includes_for_root_syncs_inventory_without_missing_sweep(
    db_session: Session,
    tmp_path: Path,
) -> None:
    roots_repo = RootsRepository(db_session)
    includes_repo = ManualIncludesRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root_path = tmp_path / "root"
    known_dir_path = root_path / "Known" / "Proj"
    nested_path = known_dir_path / "Sub"
    extras_path = root_path / "Extras"
    dormant_path = root_path / "Dormant"
    unrelated_path = root_path / "Unrelated"
    nested_path.mkdir(parents=True)
    extras_path.mkdir(parents=True)
    dormant_path.mkdir(parents=True)
    unrelated_path.mkdir(parents=True)

    unchanged_path = known_dir_path / "unchanged.txt"
    changed_path = known_dir_path / "changed.txt"
    reactivated_path = known_dir_path / "reactivated.txt"
    new_nested_path = nested_path / "new_nested.txt"
    orphan_path = extras_path / "orphan.txt"
    dormant_file_path = dormant_path / "revive.txt"
    untouched_path = unrelated_path / "untouched.txt"

    unchanged_path.write_text("same\n", encoding="utf-8")
    changed_path.write_text("changed on disk\n", encoding="utf-8")
    reactivated_path.write_text("reactivated on disk\n", encoding="utf-8")
    new_nested_path.write_text("new nested\n", encoding="utf-8")
    orphan_path.write_text("orphan\n", encoding="utf-8")
    dormant_file_path.write_text("dormant return\n", encoding="utf-8")
    untouched_path.write_text("untouched\n", encoding="utf-8")

    root = _create_root(roots_repo, path=root_path.resolve().as_posix(), name="root")
    known_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Known/Proj",
        name="Proj",
        dir_type="premiere",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )
    dormant_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Dormant",
        name="Dormant",
        dir_type="unknown",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
        is_missing=True,
    )
    unrelated_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="Unrelated",
        name="Unrelated",
        dir_type="unknown",
        first_seen_at="2026-03-14T08:00:00+00:00",
        last_seen_at="2026-03-14T08:00:00+00:00",
    )

    unchanged_stat = unchanged_path.stat()
    files_repo.create(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/unchanged.txt",
        filename="unchanged.txt",
        extension="txt",
        size_bytes=unchanged_stat.st_size,
        mtime_ns=unchanged_stat.st_mtime_ns,
        ctime_ns=unchanged_stat.st_ctime_ns,
        inode=unchanged_stat.st_ino,
        first_seen_at="2026-03-14T08:10:00+00:00",
        last_seen_at="2026-03-14T08:10:00+00:00",
    )
    files_repo.create(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/changed.txt",
        filename="changed.txt",
        extension="txt",
        size_bytes=1,
        mtime_ns=2,
        ctime_ns=3,
        inode=4,
        first_seen_at="2026-03-14T08:10:00+00:00",
        last_seen_at="2026-03-14T08:10:00+00:00",
    )
    files_repo.create(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/reactivated.txt",
        filename="reactivated.txt",
        extension="txt",
        size_bytes=5,
        mtime_ns=6,
        ctime_ns=7,
        inode=8,
        first_seen_at="2026-03-14T08:10:00+00:00",
        last_seen_at="2026-03-14T08:10:00+00:00",
        is_missing=True,
    )
    files_repo.create(
        project_dir_id=dormant_dir.id,
        relative_path="Dormant/revive.txt",
        filename="revive.txt",
        extension="txt",
        size_bytes=9,
        mtime_ns=10,
        ctime_ns=11,
        inode=12,
        first_seen_at="2026-03-14T08:20:00+00:00",
        last_seen_at="2026-03-14T08:20:00+00:00",
        is_missing=True,
    )
    untouched_stat = untouched_path.stat()
    files_repo.create(
        project_dir_id=unrelated_dir.id,
        relative_path="Unrelated/untouched.txt",
        filename="untouched.txt",
        extension="txt",
        size_bytes=untouched_stat.st_size,
        mtime_ns=untouched_stat.st_mtime_ns,
        ctime_ns=untouched_stat.st_ctime_ns,
        inode=untouched_stat.st_ino,
        first_seen_at="2026-03-14T08:30:00+00:00",
        last_seen_at="2026-03-14T08:30:00+00:00",
    )

    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="Known/Proj",
        include_path_type=IncludePathType.DIRECTORY.value,
        recursive=True,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="Extras/orphan.txt",
        include_path_type=IncludePathType.FILE.value,
        force_include=True,
    )
    _create_manual_include(
        includes_repo,
        root_id=root.id,
        relative_path="Dormant/revive.txt",
        include_path_type=IncludePathType.FILE.value,
    )

    result = apply_manual_includes_for_root(
        session=db_session,
        root_id=root.id,
        applied_at="2026-03-14T13:00:00+00:00",
    )

    items_by_path = {item.relative_path: item for item in result.item_results}
    extras_anchor = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="Extras")
    refreshed_known_dir = dirs_repo.get_by_id(known_dir.id)
    refreshed_dormant_dir = dirs_repo.get_by_id(dormant_dir.id)
    sub_dir = dirs_repo.get_by_root_and_path(root_id=root.id, relative_path="Known/Proj/Sub")
    orphan_file = None
    if extras_anchor is not None:
        orphan_file = files_repo.get_by_dir_and_path(
            project_dir_id=extras_anchor.id,
            relative_path="Extras/orphan.txt",
        )
    changed_file = files_repo.get_by_dir_and_path(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/changed.txt",
    )
    unchanged_file = files_repo.get_by_dir_and_path(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/unchanged.txt",
    )
    reactivated_file = files_repo.get_by_dir_and_path(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/reactivated.txt",
    )
    new_nested_file = files_repo.get_by_dir_and_path(
        project_dir_id=known_dir.id,
        relative_path="Known/Proj/Sub/new_nested.txt",
    )
    dormant_file = files_repo.get_by_dir_and_path(
        project_dir_id=dormant_dir.id,
        relative_path="Dormant/revive.txt",
    )
    untouched_file = files_repo.get_by_dir_and_path(
        project_dir_id=unrelated_dir.id,
        relative_path="Unrelated/untouched.txt",
    )

    assert result.processed_include_count == 3
    assert result.applied_include_count == 3
    assert result.matched_file_count == 6
    assert result.created_project_dir_count == 1
    assert result.updated_project_dir_count == 1
    assert result.reactivated_project_dir_count == 1
    assert result.created_project_file_count == 2
    assert result.updated_project_file_count == 1
    assert result.reactivated_project_file_count == 2
    assert result.unchanged_project_file_count == 1
    assert items_by_path["Extras/orphan.txt"].force_include is True

    assert extras_anchor is not None
    assert extras_anchor.is_missing is False
    assert extras_anchor.first_seen_at == "2026-03-14T13:00:00+00:00"
    assert extras_anchor.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert refreshed_known_dir is not None
    assert refreshed_known_dir.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert refreshed_dormant_dir is not None
    assert refreshed_dormant_dir.is_missing is False
    assert refreshed_dormant_dir.first_seen_at == "2026-03-14T08:00:00+00:00"
    assert refreshed_dormant_dir.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert sub_dir is None

    assert orphan_file is not None
    assert orphan_file.first_seen_at == "2026-03-14T13:00:00+00:00"
    assert orphan_file.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert changed_file is not None
    assert changed_file.size_bytes == changed_path.stat().st_size
    assert changed_file.is_missing is False
    assert changed_file.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert unchanged_file is not None
    assert unchanged_file.first_seen_at == "2026-03-14T08:10:00+00:00"
    assert unchanged_file.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert reactivated_file is not None
    assert reactivated_file.is_missing is False
    assert reactivated_file.first_seen_at == "2026-03-14T08:10:00+00:00"
    assert reactivated_file.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert new_nested_file is not None
    assert new_nested_file.project_dir_id == known_dir.id
    assert dormant_file is not None
    assert dormant_file.is_missing is False
    assert dormant_file.first_seen_at == "2026-03-14T08:20:00+00:00"
    assert dormant_file.last_seen_at == "2026-03-14T13:00:00+00:00"
    assert untouched_file is not None
    assert untouched_file.is_missing is False
    assert untouched_file.last_seen_at == "2026-03-14T08:30:00+00:00"


def _create_root(
    roots_repo: RootsRepository,
    *,
    path: str,
    name: str = "sample_root",
    is_missing: bool = False,
):
    root_stat = read_stat(path)
    return roots_repo.create(
        raid_name="raid_sample",
        name=name,
        path=path,
        device_id=None if root_stat is None else root_stat.device_id,
        inode=None if root_stat is None else root_stat.inode,
        mtime_ns=None if root_stat is None else root_stat.mtime_ns,
        ctime_ns=None if root_stat is None else root_stat.ctime_ns,
        first_seen_at="2026-03-14T09:00:00+00:00",
        last_seen_at="2026-03-14T09:00:00+00:00",
        is_missing=is_missing,
    )


def _create_manual_include(
    includes_repo: ManualIncludesRepository,
    *,
    root_id: int,
    relative_path: str,
    include_path_type: str,
    recursive: bool = False,
    force_include: bool = False,
    enabled: bool = True,
):
    return includes_repo.create(
        root_id=root_id,
        relative_path=relative_path,
        include_path_type=include_path_type,
        recursive=recursive,
        force_include=force_include,
        enabled=enabled,
        created_at="2026-03-14T09:30:00+00:00",
        updated_at="2026-03-14T09:30:00+00:00",
    )
