from pathlib import Path

import pytest

from backup_projects.adapters.filesystem.file_lock import (
    FileLockAlreadyHeldError,
    acquire_file_lock,
)


def test_acquire_file_lock_creates_missing_file_and_release_is_idempotent(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "global.lock"

    lock = acquire_file_lock(lock_path)

    assert lock_path.exists()

    lock.release()
    lock.release()


def test_acquire_file_lock_can_reacquire_after_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"

    first_lock = acquire_file_lock(lock_path)
    first_lock.release()

    second_lock = acquire_file_lock(lock_path)
    second_lock.release()


def test_acquire_file_lock_raises_when_lock_is_already_held(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"

    first_lock = acquire_file_lock(lock_path)

    with pytest.raises(
        FileLockAlreadyHeldError,
        match=r"^File lock is already held: .+global\.lock$",
    ):
        acquire_file_lock(lock_path)

    first_lock.release()


def test_acquire_file_lock_uses_os_lock_not_file_presence_as_source_of_truth(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "global.lock"
    lock_path.touch()

    with acquire_file_lock(lock_path):
        assert lock_path.exists()

    with acquire_file_lock(lock_path):
        pass
