from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from backup_projects.adapters.filesystem.path_utils import to_path


@dataclass(frozen=True, slots=True)
class DirEntryInfo:
    name: str
    path: Path
    is_dir: bool
    is_file: bool
    is_symlink: bool


def iter_dir(path: str | Path, *, include_hidden: bool = True) -> Iterator[DirEntryInfo]:
    resolved_path = to_path(path)
    with os.scandir(resolved_path) as entries:
        sorted_entries = sorted(entries, key=lambda entry: entry.name)

    for entry in sorted_entries:
        if not include_hidden and entry.name.startswith("."):
            continue

        yield DirEntryInfo(
            name=entry.name,
            path=Path(entry.path),
            is_dir=entry.is_dir(follow_symlinks=False),
            is_file=entry.is_file(follow_symlinks=False),
            is_symlink=entry.is_symlink(),
        )


def list_dir(path: str | Path, *, include_hidden: bool = True) -> tuple[DirEntryInfo, ...]:
    return tuple(iter_dir(path, include_hidden=include_hidden))
