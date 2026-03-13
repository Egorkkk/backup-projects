from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Collection, Iterator

from backup_projects.adapters.filesystem.dir_listing import list_dir
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.adapters.filesystem.stat_reader import StatInfo, read_stat


@dataclass(frozen=True, slots=True)
class FoundFileInfo:
    path: Path
    relative_path: Path


def iter_found_files(
    start_path: str | Path,
    *,
    allowed_extensions: Collection[str] | None = None,
    excluded_path_patterns: Collection[str] = (),
    stay_on_filesystem: bool = False,
    follow_symlinks: bool = False,
    include_hidden: bool = True,
) -> Iterator[FoundFileInfo]:
    normalized_start_path = resolve_path(start_path)
    start_stat = read_stat(normalized_start_path, follow_symlinks=follow_symlinks)
    if start_stat is None:
        raise FileNotFoundError(normalized_start_path)
    if not start_stat.is_dir:
        raise NotADirectoryError(normalized_start_path)

    normalized_extensions = _normalize_extensions(allowed_extensions)
    root_device_id = start_stat.device_id if stay_on_filesystem else None
    visited_dirs = {_directory_identity(start_stat)}

    yield from _walk_dir(
        normalized_start_path,
        normalized_start_path,
        normalized_extensions=normalized_extensions,
        excluded_path_patterns=tuple(excluded_path_patterns),
        root_device_id=root_device_id,
        follow_symlinks=follow_symlinks,
        include_hidden=include_hidden,
        visited_dirs=visited_dirs,
    )


def find_files(
    start_path: str | Path,
    *,
    allowed_extensions: Collection[str] | None = None,
    excluded_path_patterns: Collection[str] = (),
    stay_on_filesystem: bool = False,
    follow_symlinks: bool = False,
    include_hidden: bool = True,
) -> tuple[FoundFileInfo, ...]:
    return tuple(
        iter_found_files(
            start_path,
            allowed_extensions=allowed_extensions,
            excluded_path_patterns=excluded_path_patterns,
            stay_on_filesystem=stay_on_filesystem,
            follow_symlinks=follow_symlinks,
            include_hidden=include_hidden,
        )
    )


def _walk_dir(
    current_dir: Path,
    start_dir: Path,
    *,
    normalized_extensions: frozenset[str] | None,
    excluded_path_patterns: tuple[str, ...],
    root_device_id: int | None,
    follow_symlinks: bool,
    include_hidden: bool,
    visited_dirs: set[tuple[int | None, int | None, str]],
) -> Iterator[FoundFileInfo]:
    for entry in list_dir(current_dir, include_hidden=include_hidden):
        relative_path = entry.path.relative_to(start_dir)
        if _is_excluded(relative_path, excluded_path_patterns, is_dir=entry.is_dir):
            continue

        if entry.is_symlink and not follow_symlinks:
            continue

        stat_info = read_stat(entry.path, follow_symlinks=follow_symlinks)
        if stat_info is None:
            continue

        if root_device_id is not None and stat_info.device_id != root_device_id:
            continue

        if stat_info.is_dir:
            directory_identity = _directory_identity(stat_info)
            if directory_identity in visited_dirs:
                continue
            visited_dirs.add(directory_identity)
            yield from _walk_dir(
                entry.path,
                start_dir,
                normalized_extensions=normalized_extensions,
                excluded_path_patterns=excluded_path_patterns,
                root_device_id=root_device_id,
                follow_symlinks=follow_symlinks,
                include_hidden=include_hidden,
                visited_dirs=visited_dirs,
            )
            continue

        if stat_info.is_file and _matches_extension(entry.path, normalized_extensions):
            yield FoundFileInfo(path=entry.path, relative_path=relative_path)


def _normalize_extensions(
    extensions: Collection[str] | None,
) -> frozenset[str] | None:
    if extensions is None:
        return None
    return frozenset(extension.lower().lstrip(".") for extension in extensions)


def _matches_extension(path: Path, normalized_extensions: frozenset[str] | None) -> bool:
    if normalized_extensions is None:
        return True

    extension = path.suffix.lower().lstrip(".")
    return extension in normalized_extensions


def _is_excluded(
    relative_path: Path,
    excluded_path_patterns: tuple[str, ...],
    *,
    is_dir: bool,
) -> bool:
    relative_posix_path = relative_path.as_posix()
    candidates = (
        (relative_posix_path, f"{relative_posix_path}/") if is_dir else (relative_posix_path,)
    )
    return any(
        fnmatch(candidate, pattern)
        for pattern in excluded_path_patterns
        for candidate in candidates
    )


def _directory_identity(stat_info: StatInfo) -> tuple[int | None, int | None, str]:
    return (
        stat_info.device_id,
        stat_info.inode,
        stat_info.path.resolve(strict=False).as_posix(),
    )
