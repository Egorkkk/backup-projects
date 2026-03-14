from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class FileComparisonStatus(StrEnum):
    UNCHANGED = "unchanged"
    CHANGED = "changed"
    REACTIVATED = "reactivated"


@dataclass(frozen=True, slots=True)
class ObservedFileState:
    stored_relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None


@dataclass(frozen=True, slots=True)
class FileComparisonResult:
    status: FileComparisonStatus
    stored_relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None


def compare_project_file_state(
    *,
    existing_file,
    observed_file: ObservedFileState,
) -> FileComparisonResult:
    if observed_file.stored_relative_path != existing_file.relative_path:
        raise ValueError("Observed file path must match existing file identity path")
    _validate_required_stat_field("size_bytes", observed_file.size_bytes)
    _validate_required_stat_field("mtime_ns", observed_file.mtime_ns)
    _validate_required_stat_field("ctime_ns", observed_file.ctime_ns)

    status = FileComparisonStatus.UNCHANGED
    if existing_file.is_missing:
        status = FileComparisonStatus.REACTIVATED
    elif _did_file_stat_change(existing_file, observed_file):
        status = FileComparisonStatus.CHANGED

    return FileComparisonResult(
        status=status,
        stored_relative_path=observed_file.stored_relative_path,
        filename=observed_file.filename,
        extension=observed_file.extension,
        size_bytes=observed_file.size_bytes,
        mtime_ns=observed_file.mtime_ns,
        ctime_ns=observed_file.ctime_ns,
        inode=observed_file.inode,
    )


def _did_file_stat_change(existing_file, observed_file: ObservedFileState) -> bool:
    return (
        existing_file.size_bytes != observed_file.size_bytes
        or existing_file.mtime_ns != observed_file.mtime_ns
        or existing_file.ctime_ns != observed_file.ctime_ns
        or existing_file.inode != observed_file.inode
    )


def _validate_required_stat_field(field_name: str, value) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"Observed file field '{field_name}' must be a non-negative integer")
