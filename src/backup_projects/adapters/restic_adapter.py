from __future__ import annotations

import json
from dataclasses import dataclass

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
)
from backup_projects.adapters.process.restic_runner import (
    ResticBackupError,
    ResticBackupRequest,
    ResticCommandFailureError,
    ResticCopySnapshotRequest,
    ResticForgetKeepLastRequest,
    ResticTimeoutError,
    run_restic_backup_command,
    run_restic_copy_snapshot_command,
    run_restic_forget_keep_last_global_command,
)


@dataclass(frozen=True, slots=True)
class ResticParsedResult:
    snapshot_id: str
    summary_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class ResticBackupResult:
    manifest_file_path: str
    snapshot_id: str
    summary_payload: dict[str, object]
    argv: tuple[str, ...]
    stdout: str
    stderr: str
    duration_seconds: float


@dataclass(frozen=True, slots=True)
class ResticCopySnapshotResult:
    snapshot_id: str
    argv: tuple[str, ...]
    stdout: str
    stderr: str
    duration_seconds: float


@dataclass(frozen=True, slots=True)
class ResticForgetKeepLastResult:
    keep_last: int
    argv: tuple[str, ...]
    stdout: str
    stderr: str
    duration_seconds: float


class ResticOutputParseError(ResticBackupError):
    def __init__(self, message: str, *, stdout: str, stderr: str) -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


class ResticSnapshotIdMissingError(ResticBackupError):
    def __init__(
        self,
        *,
        summary_payload: dict[str, object],
        stdout: str,
        stderr: str,
    ) -> None:
        super().__init__("restic summary output did not contain a non-empty snapshot_id")
        self.summary_payload = summary_payload
        self.stdout = stdout
        self.stderr = stderr


def parse_restic_output(*, stdout: str, stderr: str) -> ResticParsedResult:
    summary_payload: dict[str, object] | None = None

    for line in stdout.splitlines():
        if line.strip() == "":
            continue

        try:
            parsed_line = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ResticOutputParseError(
                f"Failed to parse restic JSON output line: {line}",
                stdout=stdout,
                stderr=stderr,
            ) from exc

        if isinstance(parsed_line, dict) and parsed_line.get("message_type") == "summary":
            summary_payload = parsed_line

    if summary_payload is None:
        raise ResticOutputParseError(
            "No restic summary object found in stdout",
            stdout=stdout,
            stderr=stderr,
        )

    snapshot_id = summary_payload.get("snapshot_id")
    if not isinstance(snapshot_id, str) or snapshot_id.strip() == "":
        raise ResticSnapshotIdMissingError(
            summary_payload=summary_payload,
            stdout=stdout,
            stderr=stderr,
        )

    return ResticParsedResult(
        snapshot_id=snapshot_id,
        summary_payload=summary_payload,
    )


def run_restic_backup(
    request: ResticBackupRequest,
    *,
    runner=run_restic_backup_command,
) -> ResticBackupResult:
    try:
        command_result = runner(request)
    except CommandExitError as exc:
        raise ResticCommandFailureError(exc.result) from exc
    except CommandTimeoutError as exc:
        raise ResticTimeoutError(
            argv=exc.argv,
            timeout_seconds=exc.timeout_seconds,
            stdout=exc.stdout,
            stderr=exc.stderr,
        ) from exc

    parsed_result = parse_restic_output(
        stdout=command_result.stdout,
        stderr=command_result.stderr,
    )
    return ResticBackupResult(
        manifest_file_path=request.manifest_file_path,
        snapshot_id=parsed_result.snapshot_id,
        summary_payload=parsed_result.summary_payload,
        argv=command_result.argv,
        stdout=command_result.stdout,
        stderr=command_result.stderr,
        duration_seconds=command_result.duration_seconds,
    )


def run_restic_copy_snapshot(
    request: ResticCopySnapshotRequest,
    *,
    runner=run_restic_copy_snapshot_command,
) -> ResticCopySnapshotResult:
    command_result = _run_restic_command_request(request, runner=runner)
    return ResticCopySnapshotResult(
        snapshot_id=request.snapshot_id,
        argv=command_result.argv,
        stdout=command_result.stdout,
        stderr=command_result.stderr,
        duration_seconds=command_result.duration_seconds,
    )


def run_restic_forget_keep_last_global(
    request: ResticForgetKeepLastRequest,
    *,
    runner=run_restic_forget_keep_last_global_command,
) -> ResticForgetKeepLastResult:
    command_result = _run_restic_command_request(request, runner=runner)
    return ResticForgetKeepLastResult(
        keep_last=request.keep_last,
        argv=command_result.argv,
        stdout=command_result.stdout,
        stderr=command_result.stderr,
        duration_seconds=command_result.duration_seconds,
    )


def _run_restic_command_request(request, *, runner) -> CommandResult:
    try:
        return runner(request)
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
    "ResticBackupResult",
    "ResticCommandFailureError",
    "ResticCopySnapshotRequest",
    "ResticCopySnapshotResult",
    "ResticForgetKeepLastRequest",
    "ResticForgetKeepLastResult",
    "ResticOutputParseError",
    "ResticParsedResult",
    "ResticSnapshotIdMissingError",
    "ResticTimeoutError",
    "parse_restic_output",
    "run_restic_backup",
    "run_restic_copy_snapshot",
    "run_restic_forget_keep_last_global",
]
