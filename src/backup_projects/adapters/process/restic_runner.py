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


@dataclass(frozen=True, slots=True)
class ResticCopySnapshotRequest:
    snapshot_id: str
    binary: str
    source_repository: str
    source_password_env_var: str
    destination_repository: str
    destination_password_env_var: str
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class ResticForgetKeepLastRequest:
    keep_last: int
    binary: str
    repository: str
    password_env_var: str
    timeout_seconds: int


class ResticBackupError(RuntimeError):
    """Base restic backup adapter error."""


class ResticCommandFailureError(ResticBackupError):
    def __init__(self, result: CommandResult) -> None:
        super().__init__(f"restic command failed with return code {result.returncode}: {result.argv!r}")
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
        super().__init__(f"restic command timed out after {timeout_seconds} seconds: {argv!r}")
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
        "RESTIC_PASSWORD": _load_password_from_env(request.password_env_var),
    }

    return _run_restic_command(
        argv,
        env_overrides=env_overrides,
        timeout_seconds=request.timeout_seconds,
        command_runner=command_runner,
    )


def run_restic_copy_snapshot_command(
    request: ResticCopySnapshotRequest,
    *,
    command_runner: Callable[..., CommandResult] = run_command,
) -> CommandResult:
    argv = (
        request.binary,
        "copy",
        request.snapshot_id,
    )
    env_overrides = {
        "RESTIC_REPOSITORY": request.destination_repository,
        "RESTIC_PASSWORD": _load_password_from_env(request.destination_password_env_var),
        "RESTIC_FROM_REPOSITORY": request.source_repository,
        "RESTIC_FROM_PASSWORD": _load_password_from_env(request.source_password_env_var),
    }

    return _run_restic_command(
        argv,
        env_overrides=env_overrides,
        timeout_seconds=request.timeout_seconds,
        command_runner=command_runner,
    )


def run_restic_forget_keep_last_global_command(
    request: ResticForgetKeepLastRequest,
    *,
    command_runner: Callable[..., CommandResult] = run_command,
) -> CommandResult:
    argv = (
        request.binary,
        "forget",
        "--keep-last",
        str(request.keep_last),
        "--group-by",
        "",
        "--prune",
    )
    env_overrides = {
        "RESTIC_REPOSITORY": request.repository,
        "RESTIC_PASSWORD": _load_password_from_env(request.password_env_var),
    }

    return _run_restic_command(
        argv,
        env_overrides=env_overrides,
        timeout_seconds=request.timeout_seconds,
        command_runner=command_runner,
    )


def _load_password_from_env(env_var_name: str) -> str:
    return os.environ[env_var_name]


def _run_restic_command(
    argv: tuple[str, ...],
    *,
    env_overrides: dict[str, str],
    timeout_seconds: int,
    command_runner: Callable[..., CommandResult],
) -> CommandResult:
    try:
        return command_runner(
            argv,
            env_overrides=env_overrides,
            timeout_seconds=timeout_seconds,
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
    "ResticCopySnapshotRequest",
    "ResticCommandFailureError",
    "ResticForgetKeepLastRequest",
    "ResticTimeoutError",
    "run_restic_backup_command",
    "run_restic_copy_snapshot_command",
    "run_restic_forget_keep_last_global_command",
]
