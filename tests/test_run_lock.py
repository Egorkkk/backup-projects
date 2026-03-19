from __future__ import annotations

from pathlib import Path

import pytest

from backup_projects.adapters.filesystem.file_lock import FileLockAlreadyHeldError
from backup_projects.services.run_lock import (
    AcquiredRunLock,
    RunLockDenied,
    build_run_lock_path,
    try_acquire_run_lock,
)
from backup_projects.services.run_service import RunLifecycleRecord


def test_build_run_lock_path_returns_deterministic_global_path(tmp_path: Path) -> None:
    assert build_run_lock_path(locks_dir=tmp_path) == tmp_path / "run.lock"


def test_try_acquire_run_lock_returns_acquired_handle_on_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from backup_projects.services import run_lock as run_lock_module

    acquired_file_lock = object()
    monkeypatch.setattr(
        run_lock_module,
        "acquire_file_lock",
        lambda lock_path: acquired_file_lock,
    )

    result = try_acquire_run_lock(
        session=object(),
        run_id=17,
        locks_dir=tmp_path,
    )

    assert isinstance(result, AcquiredRunLock)
    assert result.run_id == 17
    assert result.lock_path == str(tmp_path / "run.lock")
    assert result._file_lock is acquired_file_lock


def test_try_acquire_run_lock_translates_contention_to_locked_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from backup_projects.services import run_lock as run_lock_module

    finish_calls: list[tuple[object, int, str, object | None]] = []

    def fake_finish_run(*, session, run_id, status, now=None):
        finish_calls.append((session, run_id, status, now))
        return RunLifecycleRecord(
            id=run_id,
            run_type="daily",
            status="locked",
            started_at="2026-03-20T10:00:00+00:00",
            trigger_mode="cron",
            finished_at="2026-03-20T10:00:05+00:00",
        )

    def raise_contention(lock_path):
        raise FileLockAlreadyHeldError(f"File lock is already held: {lock_path}")

    monkeypatch.setattr(run_lock_module, "acquire_file_lock", raise_contention)
    monkeypatch.setattr(run_lock_module, "finish_run", fake_finish_run)

    session = object()
    result = try_acquire_run_lock(
        session=session,
        run_id=23,
        locks_dir=tmp_path,
    )

    assert finish_calls == [(session, 23, "locked", None)]
    assert isinstance(result, RunLockDenied)
    assert result.run.id == 23
    assert result.run.status == "locked"
    assert result.lock_path == str(tmp_path / "run.lock")


def test_try_acquire_run_lock_propagates_unexpected_low_level_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from backup_projects.services import run_lock as run_lock_module

    class LockIoError(OSError):
        pass

    def raise_error(lock_path):
        raise LockIoError(f"boom: {lock_path}")

    monkeypatch.setattr(run_lock_module, "acquire_file_lock", raise_error)
    monkeypatch.setattr(
        run_lock_module,
        "finish_run",
        lambda **kwargs: pytest.fail("finish_run should not be called"),
    )

    with pytest.raises(LockIoError, match=r"^boom: .+run\.lock$"):
        try_acquire_run_lock(
            session=object(),
            run_id=31,
            locks_dir=tmp_path,
        )


def test_acquired_run_lock_release_delegates_and_is_idempotent() -> None:
    class FakeFileLock:
        def __init__(self) -> None:
            self.release_calls = 0

        def release(self) -> None:
            self.release_calls += 1

    file_lock = FakeFileLock()
    acquired_lock = AcquiredRunLock(
        run_id=41,
        lock_path="/tmp/runtime/locks/run.lock",
        _file_lock=file_lock,
    )

    acquired_lock.release()
    acquired_lock.release()

    assert file_lock.release_calls == 1

