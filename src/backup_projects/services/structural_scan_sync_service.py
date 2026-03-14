from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.structural_scan_service import StructuralScanResult


@dataclass(frozen=True, slots=True)
class StructuralScanSyncResult:
    root_id: int
    root_path: str
    synced_at: str
    scanned_project_dir_count: int
    created_project_dir_count: int
    updated_project_dir_count: int
    reactivated_project_dir_count: int
    marked_missing_project_dir_count: int
    scanned_project_file_count: int
    created_project_file_count: int
    updated_project_file_count: int
    reactivated_project_file_count: int
    marked_missing_project_file_count: int


def sync_structural_scan_result(
    *,
    session: Session,
    root_id: int,
    scan_result: StructuralScanResult,
    synced_at: str,
) -> StructuralScanSyncResult:
    roots_repo = RootsRepository(session)
    project_dirs_repo = ProjectDirsRepository(session)
    project_files_repo = ProjectFilesRepository(session)

    root_record = roots_repo.get_by_id(root_id)
    if root_record is None:
        raise LookupError(f"Root record {root_id} not found")
    if root_record.path != scan_result.root_path:
        raise ValueError(
            "Structural scan result root_path does not match the stored root path"
        )

    _validate_scan_payload(scan_result)

    existing_dirs = project_dirs_repo.list_by_root(root_id)
    existing_dirs_by_path = {record.relative_path: record for record in existing_dirs}
    scanned_dir_paths: set[str] = set()

    created_project_dir_count = 0
    updated_project_dir_count = 0
    reactivated_project_dir_count = 0
    marked_missing_project_dir_count = 0

    created_project_file_count = 0
    updated_project_file_count = 0
    reactivated_project_file_count = 0
    marked_missing_project_file_count = 0

    for scanned_dir in scan_result.project_dirs:
        scanned_dir_paths.add(scanned_dir.relative_path)
        existing_dir = existing_dirs_by_path.get(scanned_dir.relative_path)

        if existing_dir is None:
            synced_dir = project_dirs_repo.create(
                root_id=root_id,
                relative_path=scanned_dir.relative_path,
                name=scanned_dir.name,
                dir_type=scanned_dir.dir_type,
                first_seen_at=synced_at,
                last_seen_at=synced_at,
                is_missing=False,
            )
            existing_dirs_by_path[synced_dir.relative_path] = synced_dir
            created_project_dir_count += 1
        else:
            project_dirs_repo.update_scan_state(
                existing_dir.id,
                dir_type=scanned_dir.dir_type,
                is_missing=False,
                last_seen_at=synced_at,
            )
            synced_dir = existing_dir
            if existing_dir.is_missing:
                reactivated_project_dir_count += 1
            else:
                updated_project_dir_count += 1

        existing_files_by_path = {
            record.relative_path: record
            for record in project_files_repo.list_by_project_dir(synced_dir.id)
        }

        for scanned_file in scanned_dir.files:
            stored_relative_path = _build_stored_file_relative_path(
                project_dir_relative_path=scanned_dir.relative_path,
                file_relative_path=scanned_file.relative_path,
            )
            existing_file = existing_files_by_path.get(stored_relative_path)
            if existing_file is None:
                created_file = project_files_repo.create(
                    project_dir_id=synced_dir.id,
                    relative_path=stored_relative_path,
                    filename=scanned_file.filename,
                    extension=scanned_file.extension,
                    size_bytes=scanned_file.size_bytes,
                    mtime_ns=scanned_file.mtime_ns,
                    ctime_ns=scanned_file.ctime_ns,
                    inode=scanned_file.inode,
                    first_seen_at=synced_at,
                    last_seen_at=synced_at,
                    is_missing=False,
                )
                existing_files_by_path[created_file.relative_path] = created_file
                created_project_file_count += 1
                continue

            project_files_repo.update_stat_fields(
                existing_file.id,
                size_bytes=scanned_file.size_bytes,
                mtime_ns=scanned_file.mtime_ns,
                ctime_ns=scanned_file.ctime_ns,
                inode=scanned_file.inode,
                is_missing=False,
                last_seen_at=synced_at,
            )
            if existing_file.is_missing:
                reactivated_project_file_count += 1
            else:
                updated_project_file_count += 1

    for existing_dir in existing_dirs:
        if existing_dir.relative_path in scanned_dir_paths:
            continue

        project_dirs_repo.update_scan_state(
            existing_dir.id,
            dir_type=existing_dir.dir_type,
            is_missing=True,
            last_seen_at=synced_at,
        )
        marked_missing_project_dir_count += 1

        for existing_file in project_files_repo.list_by_project_dir(existing_dir.id):
            project_files_repo.mark_missing(existing_file.id, last_seen_at=synced_at)
            marked_missing_project_file_count += 1

    return StructuralScanSyncResult(
        root_id=root_id,
        root_path=scan_result.root_path,
        synced_at=synced_at,
        scanned_project_dir_count=len(scan_result.project_dirs),
        created_project_dir_count=created_project_dir_count,
        updated_project_dir_count=updated_project_dir_count,
        reactivated_project_dir_count=reactivated_project_dir_count,
        marked_missing_project_dir_count=marked_missing_project_dir_count,
        scanned_project_file_count=sum(
            len(project_dir.files) for project_dir in scan_result.project_dirs
        ),
        created_project_file_count=created_project_file_count,
        updated_project_file_count=updated_project_file_count,
        reactivated_project_file_count=reactivated_project_file_count,
        marked_missing_project_file_count=marked_missing_project_file_count,
    )


def _validate_scan_payload(scan_result: StructuralScanResult) -> None:
    seen_project_dir_paths: set[str] = set()
    for scanned_dir in scan_result.project_dirs:
        if scanned_dir.relative_path in seen_project_dir_paths:
            raise ValueError(
                f"Duplicate project_dir identity in structural scan: {scanned_dir.relative_path!r}"
            )
        seen_project_dir_paths.add(scanned_dir.relative_path)

        seen_file_paths: set[str] = set()
        for scanned_file in scanned_dir.files:
            stored_relative_path = _build_stored_file_relative_path(
                project_dir_relative_path=scanned_dir.relative_path,
                file_relative_path=scanned_file.relative_path,
            )
            if stored_relative_path in seen_file_paths:
                raise ValueError(
                    "Duplicate project_file identity in structural scan "
                    f"for project_dir {scanned_dir.relative_path!r}: {stored_relative_path!r}"
                )
            seen_file_paths.add(stored_relative_path)


def _build_stored_file_relative_path(
    *,
    project_dir_relative_path: str,
    file_relative_path: str,
) -> str:
    if project_dir_relative_path == "":
        return file_relative_path
    return f"{project_dir_relative_path}/{file_relative_path}"
