from __future__ import annotations

import errno
import fcntl
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO


class FileLockAlreadyHeldError(RuntimeError):
    """Raised when a non-blocking file lock cannot be acquired."""


@dataclass(slots=True)
class AcquiredFileLock:
    path: Path
    _handle: TextIO | None = field(repr=False)

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return

        self._handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> AcquiredFileLock:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def acquire_file_lock(lock_path: str | Path) -> AcquiredFileLock:
    normalized_lock_path = Path(lock_path)
    handle = normalized_lock_path.open("a+", encoding="utf-8")

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        if exc.errno in {errno.EACCES, errno.EAGAIN}:
            raise FileLockAlreadyHeldError(
                f"File lock is already held: {normalized_lock_path}"
            ) from exc
        raise

    return AcquiredFileLock(path=normalized_lock_path, _handle=handle)
