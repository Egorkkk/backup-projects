from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import join_path, relative_to, resolve_path
from backup_projects.adapters.filesystem.stat_reader import read_stat
from backup_projects.converters import to_manual_include
from backup_projects.domain.models import ManualInclude
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.roots_repo import RootsRepository


def create_manual_file_include(
    *,
    session: Session,
    root_id: int,
    target_path: str,
    created_at: str,
    force_include: bool = False,
    enabled: bool = True,
) -> ManualInclude:
    return _create_manual_include(
        session=session,
        root_id=root_id,
        target_path=target_path,
        include_path_type="file",
        recursive=False,
        force_include=force_include,
        enabled=enabled,
        created_at=created_at,
    )


def create_manual_directory_include(
    *,
    session: Session,
    root_id: int,
    target_path: str,
    created_at: str,
    recursive: bool = False,
    force_include: bool = False,
    enabled: bool = True,
) -> ManualInclude:
    return _create_manual_include(
        session=session,
        root_id=root_id,
        target_path=target_path,
        include_path_type="directory",
        recursive=recursive,
        force_include=force_include,
        enabled=enabled,
        created_at=created_at,
    )


def _create_manual_include(
    *,
    session: Session,
    root_id: int,
    target_path: str,
    include_path_type: str,
    recursive: bool,
    force_include: bool,
    enabled: bool,
    created_at: str,
) -> ManualInclude:
    roots_repo = RootsRepository(session)
    includes_repo = ManualIncludesRepository(session)

    root = roots_repo.get_by_id(root_id)
    if root is None:
        raise LookupError(f"Root record {root_id} not found")
    if root.is_missing:
        raise ValueError(f"Root {root.id} is marked missing")

    normalized_target_path = target_path.strip()
    if normalized_target_path == "":
        raise ValueError("Manual include path must not be empty")

    resolved_root_path = resolve_path(root.path)
    root_stat = read_stat(resolved_root_path)
    if root_stat is None:
        raise FileNotFoundError(root.path)
    if not root_stat.is_dir:
        raise NotADirectoryError(root.path)

    resolved_target_path = _resolve_target_path(
        resolved_root_path=resolved_root_path,
        target_path=normalized_target_path,
    )
    relative_path_obj = relative_to(resolved_target_path, resolved_root_path)
    if relative_path_obj is None:
        raise ValueError("Manual include target escapes the root path")

    relative_path = _normalize_relative_path(relative_path_obj)
    if relative_path == "":
        raise ValueError("Manual include path must not resolve to the root path")

    target_stat = read_stat(resolved_target_path)
    if target_stat is None:
        raise FileNotFoundError(resolved_target_path.as_posix())
    if include_path_type == "file" and not target_stat.is_file:
        raise ValueError(f"Manual include target is not a file: {resolved_target_path}")
    if include_path_type == "directory" and not target_stat.is_dir:
        raise ValueError(f"Manual include target is not a directory: {resolved_target_path}")

    duplicate = includes_repo.get_by_root_and_path(root_id=root.id, relative_path=relative_path)
    if duplicate is not None:
        raise ValueError(f"Manual include already exists for root {root.id}: {relative_path}")

    record = includes_repo.create(
        root_id=root.id,
        relative_path=relative_path,
        include_path_type=include_path_type,
        recursive=recursive,
        force_include=force_include,
        enabled=enabled,
        created_at=created_at,
        updated_at=created_at,
    )
    return to_manual_include(record)


def _resolve_target_path(*, resolved_root_path: Path, target_path: str) -> Path:
    raw_path = Path(target_path).expanduser()
    if raw_path.is_absolute():
        return resolve_path(raw_path)
    return resolve_path(join_path(resolved_root_path, target_path))


def _normalize_relative_path(path: Path) -> str:
    relative_path = path.as_posix()
    return "" if relative_path == "." else relative_path
