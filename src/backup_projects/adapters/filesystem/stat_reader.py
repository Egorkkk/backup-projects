from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from stat import S_ISDIR, S_ISLNK, S_ISREG

from backup_projects.adapters.filesystem.path_utils import to_path


@dataclass(frozen=True, slots=True)
class StatInfo:
    path: Path
    exists: bool
    is_file: bool
    is_dir: bool
    is_symlink: bool
    size_bytes: int | None
    mtime_ns: int | None
    ctime_ns: int | None
    inode: int | None
    device_id: int | None


def read_stat(path: str | Path, *, follow_symlinks: bool = False) -> StatInfo | None:
    resolved_path = to_path(path)
    try:
        stat_result = resolved_path.stat(follow_symlinks=follow_symlinks)
    except FileNotFoundError:
        return None

    mode = stat_result.st_mode
    is_file = S_ISREG(mode)
    is_dir = S_ISDIR(mode)
    is_symlink = S_ISLNK(mode)

    return StatInfo(
        path=resolved_path,
        exists=True,
        is_file=is_file,
        is_dir=is_dir,
        is_symlink=is_symlink,
        size_bytes=stat_result.st_size if is_file else None,
        mtime_ns=stat_result.st_mtime_ns,
        ctime_ns=stat_result.st_ctime_ns,
        inode=stat_result.st_ino,
        device_id=stat_result.st_dev,
    )


def path_exists(path: str | Path, *, follow_symlinks: bool = False) -> bool:
    return read_stat(path, follow_symlinks=follow_symlinks) is not None
