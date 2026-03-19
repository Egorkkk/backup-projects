from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.file_lock import (
    AcquiredFileLock,
    FileLockAlreadyHeldError,
    acquire_file_lock,
)
from backup_projects.services.run_service import RunLifecycleRecord, finish_run


@dataclass(slots=True)
class AcquiredRunLock:
    run_id: int
    lock_path: str
    _file_lock: AcquiredFileLock | None = field(repr=False)

    def release(self) -> None:
        file_lock = self._file_lock
        if file_lock is None:
            return

        self._file_lock = None
        file_lock.release()

    def __enter__(self) -> AcquiredRunLock:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class RunLockDenied:
    run: RunLifecycleRecord
    lock_path: str


def build_run_lock_path(*, locks_dir: str | Path) -> Path:
    return Path(locks_dir) / "run.lock"


def try_acquire_run_lock(
    *,
    session: Session,
    run_id: int,
    locks_dir: str | Path,
    now: Callable[[], datetime] | None = None,
) -> AcquiredRunLock | RunLockDenied:
    lock_path = build_run_lock_path(locks_dir=locks_dir)

    try:
        file_lock = acquire_file_lock(lock_path)
    except FileLockAlreadyHeldError:
        locked_run = finish_run(
            session=session,
            run_id=run_id,
            status="locked",
            now=now,
        )
        return RunLockDenied(
            run=locked_run,
            lock_path=str(lock_path),
        )

    return AcquiredRunLock(
        run_id=run_id,
        lock_path=str(lock_path),
        _file_lock=file_lock,
    )
