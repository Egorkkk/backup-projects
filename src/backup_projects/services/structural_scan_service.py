from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

from backup_projects.adapters.filesystem.file_finder import find_files
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.adapters.filesystem.stat_reader import read_stat

_PREMIERE_EXTENSIONS = frozenset({"prproj"})
_AVID_EXTENSIONS = frozenset({"avb", "avp"})
_AFTEREFFECTS_EXTENSIONS = frozenset({"aep", "aepx"})
_RESOLVE_EXTENSIONS = frozenset({"drp", "drt"})
_PRIMARY_TYPE_BY_EXTENSION = {
    **{extension: "premiere" for extension in _PREMIERE_EXTENSIONS},
    **{extension: "avid" for extension in _AVID_EXTENSIONS},
    **{extension: "aftereffects" for extension in _AFTEREFFECTS_EXTENSIONS},
    **{extension: "resolve" for extension in _RESOLVE_EXTENSIONS},
}


@dataclass(frozen=True, slots=True)
class ScannedProjectFile:
    relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None


@dataclass(frozen=True, slots=True)
class ScannedProjectDir:
    relative_path: str
    name: str
    dir_type: str
    files: tuple[ScannedProjectFile, ...]


@dataclass(frozen=True, slots=True)
class StructuralScanResult:
    root_path: str
    project_dirs: tuple[ScannedProjectDir, ...]


def scan_root_structure(
    *,
    root_path: str | Path,
    allowed_extensions: Collection[str],
    follow_symlinks: bool = False,
    stay_on_filesystem: bool = True,
) -> StructuralScanResult:
    resolved_root_path = resolve_path(root_path)
    normalized_extensions = _normalize_extensions(allowed_extensions)
    project_dir_paths = _detect_project_dir_paths(
        resolved_root_path,
        normalized_extensions=normalized_extensions,
        follow_symlinks=follow_symlinks,
        stay_on_filesystem=stay_on_filesystem,
    )

    project_dirs = tuple(
        _scan_project_dir(
            resolved_root_path,
            project_dir_path,
            follow_symlinks=follow_symlinks,
            stay_on_filesystem=stay_on_filesystem,
        )
        for project_dir_path in project_dir_paths
    )
    return StructuralScanResult(
        root_path=resolved_root_path.as_posix(),
        project_dirs=project_dirs,
    )


def _detect_project_dir_paths(
    root_path: Path,
    *,
    normalized_extensions: frozenset[str],
    follow_symlinks: bool,
    stay_on_filesystem: bool,
) -> tuple[Path, ...]:
    candidate_paths = sorted(
        {
            found_file.path.parent
            for found_file in find_files(
                root_path,
                allowed_extensions=normalized_extensions,
                follow_symlinks=follow_symlinks,
                stay_on_filesystem=stay_on_filesystem,
            )
        },
        key=lambda path: path.as_posix(),
    )

    pruned_paths: list[Path] = []
    for candidate_path in candidate_paths:
        if any(
            candidate_path != kept_path and candidate_path.is_relative_to(kept_path)
            for kept_path in pruned_paths
        ):
            continue
        pruned_paths.append(candidate_path)
    return tuple(pruned_paths)


def _scan_project_dir(
    root_path: Path,
    project_dir_path: Path,
    *,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
) -> ScannedProjectDir:
    collected_files = _collect_project_files(
        project_dir_path,
        follow_symlinks=follow_symlinks,
        stay_on_filesystem=stay_on_filesystem,
    )
    relative_path = _relative_dir_path(project_dir_path, root_path)
    return ScannedProjectDir(
        relative_path=relative_path,
        name=project_dir_path.name,
        dir_type=_classify_dir_type(collected_files),
        files=collected_files,
    )


def _collect_project_files(
    project_dir_path: Path,
    *,
    follow_symlinks: bool,
    stay_on_filesystem: bool,
) -> tuple[ScannedProjectFile, ...]:
    collected_files: list[ScannedProjectFile] = []
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
        collected_files.append(
            ScannedProjectFile(
                relative_path=found_file.relative_path.as_posix(),
                filename=found_file.path.name,
                extension=found_file.path.suffix.lower().lstrip("."),
                size_bytes=stat_info.size_bytes,
                mtime_ns=stat_info.mtime_ns,
                ctime_ns=stat_info.ctime_ns,
                inode=stat_info.inode,
            )
        )
    return tuple(collected_files)


def _classify_dir_type(files: tuple[ScannedProjectFile, ...]) -> str:
    matched_types = {
        project_type
        for file in files
        if (project_type := _PRIMARY_TYPE_BY_EXTENSION.get(file.extension))
    }
    if not matched_types:
        return "unknown"
    if len(matched_types) == 1:
        return next(iter(matched_types))
    return "mixed"


def _normalize_extensions(extensions: Collection[str]) -> frozenset[str]:
    return frozenset(extension.lower().lstrip(".") for extension in extensions)


def _relative_dir_path(project_dir_path: Path, root_path: Path) -> str:
    relative_path = project_dir_path.relative_to(root_path).as_posix()
    return "" if relative_path == "." else relative_path
