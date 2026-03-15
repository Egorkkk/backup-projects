from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.dir_listing import list_dir
from backup_projects.adapters.filesystem.file_finder import find_files
from backup_projects.adapters.filesystem.path_utils import join_path, relative_to, resolve_path
from backup_projects.adapters.filesystem.stat_reader import read_stat
from backup_projects.domain.enums import IncludePathType
from backup_projects.repositories.manual_includes_repo import (
    ManualIncludeRecord,
    ManualIncludesRepository,
)
from backup_projects.repositories.project_dirs_repo import ProjectDirRecord, ProjectDirsRepository
from backup_projects.repositories.project_files_repo import (
    ProjectFileRecord,
    ProjectFilesRepository,
)
from backup_projects.repositories.roots_repo import RootRecord, RootsRepository
from backup_projects.services.file_stat_service import (
    FileComparisonStatus,
    ObservedFileState,
    compare_project_file_state,
)


class ManualIncludeApplyStatus(StrEnum):
    APPLIED = "applied"
    SKIPPED_DISABLED = "skipped_disabled"
    MISSING_TARGET = "missing_target"
    INVALID_INCLUDE = "invalid_include"
    TYPE_MISMATCH = "type_mismatch"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class ManualIncludeApplyItemResult:
    manual_include_id: int
    root_id: int
    relative_path: str
    include_path_type: str
    recursive: bool
    force_include: bool
    status: ManualIncludeApplyStatus
    resolved_target_path: str | None
    matched_file_count: int
    skipped_file_count: int
    created_project_dir_count: int
    updated_project_dir_count: int
    reactivated_project_dir_count: int
    created_project_file_count: int
    updated_project_file_count: int
    reactivated_project_file_count: int
    unchanged_project_file_count: int
    message: str | None = None


@dataclass(frozen=True, slots=True)
class ManualIncludeScanResult:
    root_id: int
    root_path: str
    applied_at: str
    processed_include_count: int
    applied_include_count: int
    skipped_disabled_include_count: int
    missing_target_include_count: int
    invalid_include_count: int
    type_mismatch_include_count: int
    error_include_count: int
    matched_file_count: int
    skipped_file_count: int
    created_project_dir_count: int
    updated_project_dir_count: int
    reactivated_project_dir_count: int
    created_project_file_count: int
    updated_project_file_count: int
    reactivated_project_file_count: int
    unchanged_project_file_count: int
    item_results: tuple[ManualIncludeApplyItemResult, ...]


@dataclass(slots=True)
class _ItemAccumulator:
    manual_include: ManualIncludeRecord
    status: ManualIncludeApplyStatus
    resolved_target_path: str | None = None
    matched_file_count: int = 0
    skipped_file_count: int = 0
    created_project_dir_count: int = 0
    updated_project_dir_count: int = 0
    reactivated_project_dir_count: int = 0
    created_project_file_count: int = 0
    updated_project_file_count: int = 0
    reactivated_project_file_count: int = 0
    unchanged_project_file_count: int = 0
    message: str | None = None
    _dir_actions: dict[int, str] = field(default_factory=dict)

    def to_result(self) -> ManualIncludeApplyItemResult:
        return ManualIncludeApplyItemResult(
            manual_include_id=self.manual_include.id,
            root_id=self.manual_include.root_id,
            relative_path=self.manual_include.relative_path,
            include_path_type=self.manual_include.include_path_type,
            recursive=self.manual_include.recursive,
            force_include=self.manual_include.force_include,
            status=self.status,
            resolved_target_path=self.resolved_target_path,
            matched_file_count=self.matched_file_count,
            skipped_file_count=self.skipped_file_count,
            created_project_dir_count=self.created_project_dir_count,
            updated_project_dir_count=self.updated_project_dir_count,
            reactivated_project_dir_count=self.reactivated_project_dir_count,
            created_project_file_count=self.created_project_file_count,
            updated_project_file_count=self.updated_project_file_count,
            reactivated_project_file_count=self.reactivated_project_file_count,
            unchanged_project_file_count=self.unchanged_project_file_count,
            message=self.message,
        )


def apply_manual_includes_for_root(
    *,
    session: Session,
    root_id: int,
    applied_at: str,
    follow_symlinks: bool = False,
    stay_on_filesystem: bool = True,
) -> ManualIncludeScanResult:
    roots_repo = RootsRepository(session)
    includes_repo = ManualIncludesRepository(session)
    project_dirs_repo = ProjectDirsRepository(session)
    project_files_repo = ProjectFilesRepository(session)

    root = roots_repo.get_by_id(root_id)
    if root is None:
        raise LookupError(f"Root record {root_id} not found")
    if root.is_missing:
        raise ValueError(f"Root {root.id} is marked missing")

    resolved_root_path = resolve_path(root.path)
    root_stat = read_stat(resolved_root_path, follow_symlinks=follow_symlinks)
    if root_stat is None:
        raise FileNotFoundError(root.path)
    if not root_stat.is_dir:
        raise NotADirectoryError(root.path)

    manual_includes = includes_repo.list_by_root(root.id)
    project_dirs_by_path = {
        record.relative_path: record for record in project_dirs_repo.list_by_root(root.id)
    }
    project_files_by_dir_id: dict[int, dict[str, ProjectFileRecord]] = {}
    item_results: list[ManualIncludeApplyItemResult] = []

    for manual_include in manual_includes:
        item_results.append(
            _apply_manual_include(
                manual_include=manual_include,
                root=root,
                resolved_root_path=resolved_root_path,
                root_device_id=root_stat.device_id,
                applied_at=applied_at,
                follow_symlinks=follow_symlinks,
                stay_on_filesystem=stay_on_filesystem,
                project_dirs_repo=project_dirs_repo,
                project_files_repo=project_files_repo,
                project_dirs_by_path=project_dirs_by_path,
                project_files_by_dir_id=project_files_by_dir_id,
            )
        )

    return ManualIncludeScanResult(
        root_id=root.id,
        root_path=resolved_root_path.as_posix(),
        applied_at=applied_at,
        processed_include_count=len(item_results),
        applied_include_count=sum(
            result.status == ManualIncludeApplyStatus.APPLIED for result in item_results
        ),
        skipped_disabled_include_count=sum(
            result.status == ManualIncludeApplyStatus.SKIPPED_DISABLED for result in item_results
        ),
        missing_target_include_count=sum(
            result.status == ManualIncludeApplyStatus.MISSING_TARGET for result in item_results
        ),
        invalid_include_count=sum(
            result.status == ManualIncludeApplyStatus.INVALID_INCLUDE for result in item_results
        ),
        type_mismatch_include_count=sum(
            result.status == ManualIncludeApplyStatus.TYPE_MISMATCH for result in item_results
        ),
        error_include_count=sum(
            result.status == ManualIncludeApplyStatus.ERROR for result in item_results
        ),
        matched_file_count=sum(result.matched_file_count for result in item_results),
        skipped_file_count=sum(result.skipped_file_count for result in item_results),
        created_project_dir_count=sum(
            result.created_project_dir_count for result in item_results
        ),
        updated_project_dir_count=sum(
            result.updated_project_dir_count for result in item_results
        ),
        reactivated_project_dir_count=sum(
            result.reactivated_project_dir_count for result in item_results
        ),
        created_project_file_count=sum(
            result.created_project_file_count for result in item_results
        ),
        updated_project_file_count=sum(
            result.updated_project_file_count for result in item_results
        ),
        reactivated_project_file_count=sum(
            result.reactivated_project_file_count for result in item_results
        ),
        unchanged_project_file_count=sum(
            result.unchanged_project_file_count for result in item_results
        ),
        item_results=tuple(item_results),
    )


def _apply_manual_include(
    *,
    manual_include: ManualIncludeRecord,
    root: RootRecord,
    resolved_root_path: Path,
    root_device_id: int | None,
    applied_at: str,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
    project_dirs_repo: ProjectDirsRepository,
    project_files_repo: ProjectFilesRepository,
    project_dirs_by_path: dict[str, ProjectDirRecord],
    project_files_by_dir_id: dict[int, dict[str, ProjectFileRecord]],
) -> ManualIncludeApplyItemResult:
    item = _ItemAccumulator(
        manual_include=manual_include,
        status=ManualIncludeApplyStatus.APPLIED,
    )

    if not manual_include.enabled:
        item.status = ManualIncludeApplyStatus.SKIPPED_DISABLED
        item.message = "Manual include is disabled"
        return item.to_result()

    if manual_include.relative_path == "":
        item.status = ManualIncludeApplyStatus.INVALID_INCLUDE
        item.message = "Manual include path must not be empty"
        return item.to_result()

    try:
        include_path_type = IncludePathType(manual_include.include_path_type)
    except ValueError:
        item.status = ManualIncludeApplyStatus.INVALID_INCLUDE
        item.message = f"Invalid include_path_type: {manual_include.include_path_type}"
        return item.to_result()

    try:
        resolved_target_path, normalized_target_relative_path = _resolve_manual_include_target(
            resolved_root_path=resolved_root_path,
            relative_path=manual_include.relative_path,
        )
    except ValueError as exc:
        item.status = ManualIncludeApplyStatus.INVALID_INCLUDE
        item.message = str(exc)
        return item.to_result()

    item.resolved_target_path = resolved_target_path.as_posix()
    target_stat = read_stat(resolved_target_path, follow_symlinks=follow_symlinks)
    if target_stat is None:
        item.status = ManualIncludeApplyStatus.MISSING_TARGET
        item.message = "Manual include target is missing"
        return item.to_result()

    if include_path_type is IncludePathType.FILE:
        if not target_stat.is_file:
            item.status = ManualIncludeApplyStatus.TYPE_MISMATCH
            item.message = "Manual include target is not a file"
            return item.to_result()
        try:
            _apply_observed_file_path(
                file_path=resolved_target_path,
                fallback_anchor_relative_path=_parent_relative_path(
                    normalized_target_relative_path
                ),
                root=root,
                resolved_root_path=resolved_root_path,
                root_device_id=root_device_id,
                applied_at=applied_at,
                follow_symlinks=follow_symlinks,
                stay_on_filesystem=stay_on_filesystem,
                project_dirs_repo=project_dirs_repo,
                project_files_repo=project_files_repo,
                project_dirs_by_path=project_dirs_by_path,
                project_files_by_dir_id=project_files_by_dir_id,
                item=item,
            )
        except OSError as exc:
            item.status = ManualIncludeApplyStatus.ERROR
            item.message = str(exc)
        return item.to_result()

    if not target_stat.is_dir:
        item.status = ManualIncludeApplyStatus.TYPE_MISMATCH
        item.message = "Manual include target is not a directory"
        return item.to_result()

    try:
        _ensure_project_dir_anchor(
            root=root,
            candidate_relative_path=normalized_target_relative_path,
            applied_at=applied_at,
            project_dirs_repo=project_dirs_repo,
            project_dirs_by_path=project_dirs_by_path,
            item=item,
        )

        if manual_include.recursive:
            found_files = find_files(
                resolved_target_path,
                allowed_extensions=None,
                follow_symlinks=follow_symlinks,
                stay_on_filesystem=stay_on_filesystem,
            )
            for found_file in found_files:
                _apply_observed_file_path(
                    file_path=found_file.path,
                    fallback_anchor_relative_path=normalized_target_relative_path,
                    root=root,
                    resolved_root_path=resolved_root_path,
                    root_device_id=root_device_id,
                    applied_at=applied_at,
                    follow_symlinks=follow_symlinks,
                    stay_on_filesystem=stay_on_filesystem,
                    project_dirs_repo=project_dirs_repo,
                    project_files_repo=project_files_repo,
                    project_dirs_by_path=project_dirs_by_path,
                    project_files_by_dir_id=project_files_by_dir_id,
                    item=item,
                )
        else:
            for entry in list_dir(resolved_target_path):
                _apply_observed_file_path(
                    file_path=entry.path,
                    fallback_anchor_relative_path=normalized_target_relative_path,
                    root=root,
                    resolved_root_path=resolved_root_path,
                    root_device_id=root_device_id,
                    applied_at=applied_at,
                    follow_symlinks=follow_symlinks,
                    stay_on_filesystem=stay_on_filesystem,
                    project_dirs_repo=project_dirs_repo,
                    project_files_repo=project_files_repo,
                    project_dirs_by_path=project_dirs_by_path,
                    project_files_by_dir_id=project_files_by_dir_id,
                    item=item,
                )
    except OSError as exc:
        item.status = ManualIncludeApplyStatus.ERROR
        item.message = str(exc)

    return item.to_result()


def _resolve_manual_include_target(
    *,
    resolved_root_path: Path,
    relative_path: str,
) -> tuple[Path, str]:
    resolved_target_path = resolve_path(join_path(resolved_root_path, relative_path))
    relative_path_obj = relative_to(resolved_target_path, resolved_root_path)
    if relative_path_obj is None:
        raise ValueError("Manual include target escapes the root path")
    return resolved_target_path, _normalize_inventory_relative_path(relative_path_obj)


def _apply_observed_file_path(
    *,
    file_path: Path,
    fallback_anchor_relative_path: str,
    root: RootRecord,
    resolved_root_path: Path,
    root_device_id: int | None,
    applied_at: str,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
    project_dirs_repo: ProjectDirsRepository,
    project_files_repo: ProjectFilesRepository,
    project_dirs_by_path: dict[str, ProjectDirRecord],
    project_files_by_dir_id: dict[int, dict[str, ProjectFileRecord]],
    item: _ItemAccumulator,
) -> None:
    observed_file = _build_observed_file_state(
        file_path=file_path,
        resolved_root_path=resolved_root_path,
        root_device_id=root_device_id,
        follow_symlinks=follow_symlinks,
        stay_on_filesystem=stay_on_filesystem,
    )
    if observed_file is None:
        item.skipped_file_count += 1
        return

    item.matched_file_count += 1
    project_dir = _find_longest_prefix_project_dir(
        file_relative_path=observed_file.stored_relative_path,
        project_dirs_by_path=project_dirs_by_path,
    )
    if project_dir is None:
        project_dir = _ensure_project_dir_anchor(
            root=root,
            candidate_relative_path=fallback_anchor_relative_path,
            applied_at=applied_at,
            project_dirs_repo=project_dirs_repo,
            project_dirs_by_path=project_dirs_by_path,
            item=item,
        )
    else:
        project_dir = _ensure_project_dir_anchor(
            root=root,
            candidate_relative_path=project_dir.relative_path,
            applied_at=applied_at,
            project_dirs_repo=project_dirs_repo,
            project_dirs_by_path=project_dirs_by_path,
            item=item,
        )

    project_files_by_path = _get_project_files_by_path(
        project_dir_id=project_dir.id,
        project_files_repo=project_files_repo,
        project_files_by_dir_id=project_files_by_dir_id,
    )
    existing_file = project_files_by_path.get(observed_file.stored_relative_path)
    if existing_file is None:
        created_file = project_files_repo.create(
            project_dir_id=project_dir.id,
            relative_path=observed_file.stored_relative_path,
            filename=observed_file.filename,
            extension=observed_file.extension,
            size_bytes=observed_file.size_bytes,
            mtime_ns=observed_file.mtime_ns,
            ctime_ns=observed_file.ctime_ns,
            inode=observed_file.inode,
            first_seen_at=applied_at,
            last_seen_at=applied_at,
            is_missing=False,
        )
        project_files_by_path[created_file.relative_path] = created_file
        item.created_project_file_count += 1
        return

    comparison = compare_project_file_state(
        existing_file=existing_file,
        observed_file=observed_file,
    )
    if comparison.status is FileComparisonStatus.REACTIVATED:
        project_files_repo.update_stat_fields(
            existing_file.id,
            size_bytes=comparison.size_bytes,
            mtime_ns=comparison.mtime_ns,
            ctime_ns=comparison.ctime_ns,
            inode=comparison.inode,
            is_missing=False,
            last_seen_at=applied_at,
        )
        refreshed_file = project_files_repo.get_by_id(existing_file.id)
        if refreshed_file is None:
            raise RuntimeError("Failed to refresh reactivated project_file record")
        project_files_by_path[refreshed_file.relative_path] = refreshed_file
        item.reactivated_project_file_count += 1
        return

    if comparison.status is FileComparisonStatus.CHANGED:
        project_files_repo.update_stat_fields(
            existing_file.id,
            size_bytes=comparison.size_bytes,
            mtime_ns=comparison.mtime_ns,
            ctime_ns=comparison.ctime_ns,
            inode=comparison.inode,
            is_missing=False,
            last_seen_at=applied_at,
        )
        refreshed_file = project_files_repo.get_by_id(existing_file.id)
        if refreshed_file is None:
            raise RuntimeError("Failed to refresh updated project_file record")
        project_files_by_path[refreshed_file.relative_path] = refreshed_file
        item.updated_project_file_count += 1
        return

    project_files_repo.update_stat_fields(
        existing_file.id,
        size_bytes=existing_file.size_bytes,
        mtime_ns=existing_file.mtime_ns,
        ctime_ns=existing_file.ctime_ns,
        inode=existing_file.inode,
        is_missing=False,
        last_seen_at=applied_at,
    )
    refreshed_file = project_files_repo.get_by_id(existing_file.id)
    if refreshed_file is None:
        raise RuntimeError("Failed to refresh unchanged project_file record")
    project_files_by_path[refreshed_file.relative_path] = refreshed_file
    item.unchanged_project_file_count += 1


def _build_observed_file_state(
    *,
    file_path: Path,
    resolved_root_path: Path,
    root_device_id: int | None,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
) -> ObservedFileState | None:
    try:
        stat_info = read_stat(file_path, follow_symlinks=follow_symlinks)
    except OSError:
        return None
    if stat_info is None or not stat_info.is_file:
        return None
    if stay_on_filesystem and root_device_id is not None and stat_info.device_id != root_device_id:
        return None
    if (
        stat_info.size_bytes is None
        or stat_info.mtime_ns is None
        or stat_info.ctime_ns is None
    ):
        return None

    relative_path_obj = relative_to(file_path, resolved_root_path)
    if relative_path_obj is None:
        raise RuntimeError("Observed file path escaped the root unexpectedly")

    stored_relative_path = _normalize_inventory_relative_path(relative_path_obj)
    return ObservedFileState(
        stored_relative_path=stored_relative_path,
        filename=file_path.name,
        extension=file_path.suffix.lower().lstrip("."),
        size_bytes=stat_info.size_bytes,
        mtime_ns=stat_info.mtime_ns,
        ctime_ns=stat_info.ctime_ns,
        inode=stat_info.inode,
    )


def _ensure_project_dir_anchor(
    *,
    root: RootRecord,
    candidate_relative_path: str,
    applied_at: str,
    project_dirs_repo: ProjectDirsRepository,
    project_dirs_by_path: dict[str, ProjectDirRecord],
    item: _ItemAccumulator,
) -> ProjectDirRecord:
    existing_record = project_dirs_by_path.get(candidate_relative_path)
    if existing_record is None:
        created_record = project_dirs_repo.create(
            root_id=root.id,
            relative_path=candidate_relative_path,
            name=_project_dir_name(candidate_relative_path, root.name),
            dir_type="unknown",
            first_seen_at=applied_at,
            last_seen_at=applied_at,
            is_missing=False,
        )
        project_dirs_by_path[created_record.relative_path] = created_record
        item.created_project_dir_count += 1
        item._dir_actions[created_record.id] = "created"
        return created_record

    if existing_record.id in item._dir_actions:
        return existing_record

    if existing_record.is_missing:
        project_dirs_repo.update_scan_state(
            existing_record.id,
            dir_type=existing_record.dir_type,
            is_missing=False,
            last_seen_at=applied_at,
        )
        refreshed_record = project_dirs_repo.get_by_id(existing_record.id)
        if refreshed_record is None:
            raise RuntimeError("Failed to refresh reactivated project_dir record")
        project_dirs_by_path[refreshed_record.relative_path] = refreshed_record
        item.reactivated_project_dir_count += 1
        item._dir_actions[refreshed_record.id] = "reactivated"
        return refreshed_record

    project_dirs_repo.update_scan_state(
        existing_record.id,
        dir_type=existing_record.dir_type,
        is_missing=False,
        last_seen_at=applied_at,
    )
    refreshed_record = project_dirs_repo.get_by_id(existing_record.id)
    if refreshed_record is None:
        raise RuntimeError("Failed to refresh updated project_dir record")
    project_dirs_by_path[refreshed_record.relative_path] = refreshed_record
    item.updated_project_dir_count += 1
    item._dir_actions[refreshed_record.id] = "updated"
    return refreshed_record


def _find_longest_prefix_project_dir(
    *,
    file_relative_path: str,
    project_dirs_by_path: dict[str, ProjectDirRecord],
) -> ProjectDirRecord | None:
    matches = [
        record
        for relative_path, record in project_dirs_by_path.items()
        if _path_contains_file(relative_path=relative_path, file_relative_path=file_relative_path)
    ]
    if not matches:
        return None
    return max(matches, key=lambda record: len(record.relative_path))


def _path_contains_file(*, relative_path: str, file_relative_path: str) -> bool:
    if relative_path == "":
        return True
    return file_relative_path == relative_path or file_relative_path.startswith(
        f"{relative_path}/"
    )


def _get_project_files_by_path(
    *,
    project_dir_id: int,
    project_files_repo: ProjectFilesRepository,
    project_files_by_dir_id: dict[int, dict[str, ProjectFileRecord]],
) -> dict[str, ProjectFileRecord]:
    if project_dir_id not in project_files_by_dir_id:
        project_files_by_dir_id[project_dir_id] = {
            record.relative_path: record
            for record in project_files_repo.list_by_project_dir(project_dir_id)
        }
    return project_files_by_dir_id[project_dir_id]


def _normalize_inventory_relative_path(path: Path) -> str:
    relative_path = path.as_posix()
    return "" if relative_path == "." else relative_path


def _parent_relative_path(file_relative_path: str) -> str:
    return _normalize_inventory_relative_path(Path(file_relative_path).parent)


def _project_dir_name(candidate_relative_path: str, root_name: str) -> str:
    if candidate_relative_path == "":
        return root_name
    return Path(candidate_relative_path).name
