from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.file_finder import find_files
from backup_projects.adapters.filesystem.path_utils import join_path, resolve_path
from backup_projects.adapters.filesystem.stat_reader import read_stat
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.file_stat_service import (
    FileComparisonStatus,
    ObservedFileState,
    compare_project_file_state,
)


@dataclass(frozen=True, slots=True)
class ProjectDirIncrementalScanResult:
    project_dir_id: int
    root_id: int
    project_dir_relative_path: str
    project_dir_path: str
    scanned_at: str
    project_dir_present: bool
    scanned_file_count: int
    new_file_count: int
    changed_file_count: int
    reactivated_file_count: int
    missing_file_count: int
    unchanged_file_count: int


@dataclass(frozen=True, slots=True)
class _ObservedFile:
    stored_relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None


def scan_and_sync_project_dir(
    *,
    session: Session,
    project_dir_id: int,
    scanned_at: str,
    follow_symlinks: bool = False,
    stay_on_filesystem: bool = True,
) -> ProjectDirIncrementalScanResult:
    project_dirs_repo = ProjectDirsRepository(session)
    project_files_repo = ProjectFilesRepository(session)
    roots_repo = RootsRepository(session)

    project_dir = project_dirs_repo.get_by_id(project_dir_id)
    if project_dir is None:
        raise LookupError(f"Project dir record {project_dir_id} not found")
    if project_dir.is_missing:
        raise ValueError(f"Project dir {project_dir_id} is marked missing")

    root = roots_repo.get_by_id(project_dir.root_id)
    if root is None:
        raise LookupError(f"Root record {project_dir.root_id} not found")
    if root.is_missing:
        raise ValueError(f"Root {root.id} is marked missing")

    project_dir_path = _resolve_project_dir_path(
        root_path=root.path,
        project_dir_relative_path=project_dir.relative_path,
    )
    target_stat = read_stat(project_dir_path, follow_symlinks=follow_symlinks)
    if target_stat is None:
        observed_files_by_path: dict[str, _ObservedFile] = {}
        project_dir_present = False
    else:
        if not target_stat.is_dir:
            raise NotADirectoryError(project_dir_path)
        observed_files_by_path = _collect_observed_files(
            project_dir_path=project_dir_path,
            project_dir_relative_path=project_dir.relative_path,
            follow_symlinks=follow_symlinks,
            stay_on_filesystem=stay_on_filesystem,
        )
        project_dir_present = True

    existing_files_by_path = {
        record.relative_path: record
        for record in project_files_repo.list_by_project_dir(project_dir_id)
    }

    new_file_count = 0
    changed_file_count = 0
    reactivated_file_count = 0
    missing_file_count = 0
    unchanged_file_count = 0

    for stored_relative_path, observed_file in observed_files_by_path.items():
        existing_file = existing_files_by_path.get(stored_relative_path)
        if existing_file is None:
            project_files_repo.create(
                project_dir_id=project_dir_id,
                relative_path=observed_file.stored_relative_path,
                filename=observed_file.filename,
                extension=observed_file.extension,
                size_bytes=observed_file.size_bytes,
                mtime_ns=observed_file.mtime_ns,
                ctime_ns=observed_file.ctime_ns,
                inode=observed_file.inode,
                first_seen_at=scanned_at,
                last_seen_at=scanned_at,
                is_missing=False,
            )
            new_file_count += 1
            continue

        comparison = compare_project_file_state(
            existing_file=existing_file,
            observed_file=ObservedFileState(
                stored_relative_path=observed_file.stored_relative_path,
                filename=observed_file.filename,
                extension=observed_file.extension,
                size_bytes=observed_file.size_bytes,
                mtime_ns=observed_file.mtime_ns,
                ctime_ns=observed_file.ctime_ns,
                inode=observed_file.inode,
            ),
        )

        if comparison.status == FileComparisonStatus.REACTIVATED:
            project_files_repo.update_stat_fields(
                existing_file.id,
                size_bytes=comparison.size_bytes,
                mtime_ns=comparison.mtime_ns,
                ctime_ns=comparison.ctime_ns,
                inode=comparison.inode,
                is_missing=False,
                last_seen_at=scanned_at,
            )
            reactivated_file_count += 1
            continue

        if comparison.status == FileComparisonStatus.CHANGED:
            project_files_repo.update_stat_fields(
                existing_file.id,
                size_bytes=comparison.size_bytes,
                mtime_ns=comparison.mtime_ns,
                ctime_ns=comparison.ctime_ns,
                inode=comparison.inode,
                is_missing=False,
                last_seen_at=scanned_at,
            )
            changed_file_count += 1
            continue

        project_files_repo.update_stat_fields(
            existing_file.id,
            size_bytes=existing_file.size_bytes,
            mtime_ns=existing_file.mtime_ns,
            ctime_ns=existing_file.ctime_ns,
            inode=existing_file.inode,
            is_missing=False,
            last_seen_at=scanned_at,
        )
        unchanged_file_count += 1

    for existing_file in existing_files_by_path.values():
        if existing_file.relative_path in observed_files_by_path:
            continue
        project_files_repo.mark_missing(existing_file.id, last_seen_at=scanned_at)
        missing_file_count += 1

    return ProjectDirIncrementalScanResult(
        project_dir_id=project_dir.id,
        root_id=root.id,
        project_dir_relative_path=project_dir.relative_path,
        project_dir_path=project_dir_path,
        scanned_at=scanned_at,
        project_dir_present=project_dir_present,
        scanned_file_count=len(observed_files_by_path),
        new_file_count=new_file_count,
        changed_file_count=changed_file_count,
        reactivated_file_count=reactivated_file_count,
        missing_file_count=missing_file_count,
        unchanged_file_count=unchanged_file_count,
    )


def _resolve_project_dir_path(*, root_path: str, project_dir_relative_path: str) -> str:
    if project_dir_relative_path == "":
        return resolve_path(root_path).as_posix()
    return resolve_path(join_path(root_path, project_dir_relative_path)).as_posix()


def _collect_observed_files(
    *,
    project_dir_path: str,
    project_dir_relative_path: str,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
) -> dict[str, _ObservedFile]:
    observed_files_by_path: dict[str, _ObservedFile] = {}
    for found_file in find_files(
        project_dir_path,
        allowed_extensions=None,
        follow_symlinks=follow_symlinks,
        stay_on_filesystem=stay_on_filesystem,
    ):
        stat_info = read_stat(found_file.path, follow_symlinks=follow_symlinks)
        if stat_info is None or not stat_info.is_file:
            continue
        if (
            stat_info.size_bytes is None
            or stat_info.mtime_ns is None
            or stat_info.ctime_ns is None
        ):
            continue

        stored_relative_path = _build_stored_file_relative_path(
            project_dir_relative_path=project_dir_relative_path,
            file_relative_path=found_file.relative_path.as_posix(),
        )
        observed_files_by_path[stored_relative_path] = _ObservedFile(
            stored_relative_path=stored_relative_path,
            filename=found_file.path.name,
            extension=found_file.path.suffix.lower().lstrip("."),
            size_bytes=stat_info.size_bytes,
            mtime_ns=stat_info.mtime_ns,
            ctime_ns=stat_info.ctime_ns,
            inode=stat_info.inode,
        )
    return observed_files_by_path


def _build_stored_file_relative_path(
    *,
    project_dir_relative_path: str,
    file_relative_path: str,
) -> str:
    if project_dir_relative_path == "":
        return file_relative_path
    return f"{project_dir_relative_path}/{file_relative_path}"
