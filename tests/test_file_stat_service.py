from __future__ import annotations

from types import SimpleNamespace

import pytest

from backup_projects.services.file_stat_service import (
    FileComparisonStatus,
    ObservedFileState,
    compare_project_file_state,
)


def test_compare_project_file_state_returns_unchanged_and_mirrors_observed_payload() -> None:
    existing_file = _existing_file()
    observed_file = _observed_file()

    result = compare_project_file_state(
        existing_file=existing_file,
        observed_file=observed_file,
    )

    assert result.status == FileComparisonStatus.UNCHANGED
    assert result.stored_relative_path == observed_file.stored_relative_path
    assert result.filename == observed_file.filename
    assert result.extension == observed_file.extension
    assert result.size_bytes == observed_file.size_bytes
    assert result.mtime_ns == observed_file.mtime_ns
    assert result.ctime_ns == observed_file.ctime_ns
    assert result.inode == observed_file.inode


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("size_bytes", 999),
        ("mtime_ns", 888),
        ("ctime_ns", 777),
        ("inode", 666),
    ],
)
def test_compare_project_file_state_returns_changed_for_any_stat_difference(
    field_name: str,
    field_value: int,
) -> None:
    existing_file = _existing_file()
    observed_file = _observed_file(**{field_name: field_value})

    result = compare_project_file_state(
        existing_file=existing_file,
        observed_file=observed_file,
    )

    assert result.status == FileComparisonStatus.CHANGED


def test_compare_project_file_state_returns_reactivated_for_missing_existing_file() -> None:
    existing_file = _existing_file(is_missing=True)
    observed_file = _observed_file(size_bytes=999, mtime_ns=888, ctime_ns=777, inode=666)

    result = compare_project_file_state(
        existing_file=existing_file,
        observed_file=observed_file,
    )

    assert result.status == FileComparisonStatus.REACTIVATED


def test_compare_project_file_state_raises_for_mismatched_stored_path() -> None:
    existing_file = _existing_file(relative_path="Project A/clip.mov")
    observed_file = _observed_file(stored_relative_path="Project A/other.mov")

    with pytest.raises(
        ValueError,
        match="Observed file path must match existing file identity path",
    ):
        compare_project_file_state(
            existing_file=existing_file,
            observed_file=observed_file,
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("size_bytes", None),
        ("size_bytes", "12"),
        ("size_bytes", -1),
        ("mtime_ns", None),
        ("mtime_ns", "34"),
        ("mtime_ns", -1),
        ("ctime_ns", None),
        ("ctime_ns", "56"),
        ("ctime_ns", -1),
    ],
)
def test_compare_project_file_state_raises_for_invalid_required_stat_fields(
    field_name: str,
    field_value: object,
) -> None:
    existing_file = _existing_file()
    observed_file = _observed_file(**{field_name: field_value})

    with pytest.raises(ValueError, match=field_name):
        compare_project_file_state(
            existing_file=existing_file,
            observed_file=observed_file,
        )


def _existing_file(
    *,
    relative_path: str = "Project A/clip.mov",
    size_bytes: int = 123,
    mtime_ns: int = 456,
    ctime_ns: int = 789,
    inode: int | None = 101112,
    is_missing: bool = False,
):
    return SimpleNamespace(
        relative_path=relative_path,
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
        ctime_ns=ctime_ns,
        inode=inode,
        is_missing=is_missing,
    )


def _observed_file(
    *,
    stored_relative_path: str = "Project A/clip.mov",
    filename: str = "clip.mov",
    extension: str = "mov",
    size_bytes: object = 123,
    mtime_ns: object = 456,
    ctime_ns: object = 789,
    inode: int | None = 101112,
) -> ObservedFileState:
    return ObservedFileState(
        stored_relative_path=stored_relative_path,
        filename=filename,
        extension=extension,
        size_bytes=size_bytes,  # type: ignore[arg-type]
        mtime_ns=mtime_ns,  # type: ignore[arg-type]
        ctime_ns=ctime_ns,  # type: ignore[arg-type]
        inode=inode,
    )
