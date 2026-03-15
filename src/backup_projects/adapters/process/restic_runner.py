from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
    run_command,
)


@dataclass(frozen=True, slots=True)
class ResticBackupRequest:
    manifest_file_path: str
    binary: str
    repository: str
    password_env_var: str
    timeout_seconds: int


class ResticBackupError(RuntimeError):
    """Base restic backup adapter error."""


class ResticCommandFailureError(ResticBackupError):
    def __init__(self, result: CommandResult) -> None:
        super().__init__(
            f"restic backup failed with return code {result.returncode}: {result.argv!r}"
        )
        self.argv = result.argv
        self.returncode = result.returncode
        self.stdout = result.stdout
        self.stderr = result.stderr
        self.duration_seconds = result.duration_seconds


class ResticTimeoutError(ResticBackupError):
    def __init__(
        self,
        *,
        argv: tuple[str, ...],
        timeout_seconds: float,
        stdout: str | None,
        stderr: str | None,
    ) -> None:
        super().__init__(f"restic backup timed out after {timeout_seconds} seconds: {argv!r}")
        self.argv = argv
        self.timeout_seconds = timeout_seconds
        self.stdout = stdout
        self.stderr = stderr


def run_restic_backup_command(
    request: ResticBackupRequest,
    *,
    command_runner: Callable[..., CommandResult] = run_command,
) -> CommandResult:
    argv = (
        request.binary,
        "backup",
        "--json",
        "--files-from-verbatim",
        request.manifest_file_path,
    )
    env_overrides = {
        "RESTIC_REPOSITORY": request.repository,
        "RESTIC_PASSWORD": os.environ[request.password_env_var],
    }

    try:
        return command_runner(
            argv,
            env_overrides=env_overrides,
            timeout_seconds=request.timeout_seconds,
            check=True,
        )
    except CommandExitError as exc:
        raise ResticCommandFailureError(exc.result) from exc
    except CommandTimeoutError as exc:
        raise ResticTimeoutError(
            argv=exc.argv,
            timeout_seconds=exc.timeout_seconds,
            stdout=exc.stdout,
            stderr=exc.stderr,
        ) from exc


__all__ = [
    "ResticBackupError",
    "ResticBackupRequest",
    "ResticCommandFailureError",
    "ResticTimeoutError",
    "run_restic_backup_command",
]
