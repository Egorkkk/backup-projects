from __future__ import annotations

from pathlib import Path


def to_path(value: str | Path) -> Path:
    return value if isinstance(value, Path) else Path(value)


def resolve_path(path: str | Path) -> Path:
    return to_path(path).expanduser().resolve(strict=False)


def join_path(base: str | Path, *parts: str) -> Path:
    path = to_path(base)
    for part in parts:
        path = path / part
    return path


def relative_to(path: str | Path, base: str | Path) -> Path | None:
    resolved_path = resolve_path(path)
    resolved_base = resolve_path(base)
    try:
        return resolved_path.relative_to(resolved_base)
    except ValueError:
        return None


def is_relative_to(path: str | Path, base: str | Path) -> bool:
    return relative_to(path, base) is not None


def is_same_filesystem(path_a: str | Path, path_b: str | Path) -> bool:
    left = to_path(path_a).stat(follow_symlinks=False)
    right = to_path(path_b).stat(follow_symlinks=False)
    return left.st_dev == right.st_dev
