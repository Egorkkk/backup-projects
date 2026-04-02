from __future__ import annotations

from dataclasses import dataclass

from backup_projects.adapters.restic_adapter import (
    ResticCopySnapshotRequest,
    ResticCopySnapshotResult,
    ResticForgetKeepLastRequest,
    ResticForgetKeepLastResult,
    run_restic_copy_snapshot,
    run_restic_forget_keep_last_global,
)


@dataclass(frozen=True, slots=True)
class PostBackupArchiveRequest:
    snapshot_id: str
    restic_binary: str
    local_repository: str
    local_password_env_var: str
    remote_repository: str
    remote_password_env_var: str
    timeout_seconds: int
    local_retention_keep_last: int


@dataclass(frozen=True, slots=True)
class PostBackupArchiveResult:
    snapshot_id: str
    remote_repository: str
    local_retention_keep_last: int
    archive_status: str
    retention_status: str
    archive_result: ResticCopySnapshotResult | None = None
    retention_result: ResticForgetKeepLastResult | None = None
    archive_error: str | None = None
    retention_error: str | None = None
    archive_exception: Exception | None = None
    retention_exception: Exception | None = None


def run_post_backup_archive(
    request: PostBackupArchiveRequest,
    *,
    archive_runner=run_restic_copy_snapshot,
    retention_runner=run_restic_forget_keep_last_global,
) -> PostBackupArchiveResult:
    try:
        archive_result = archive_runner(
            ResticCopySnapshotRequest(
                snapshot_id=request.snapshot_id,
                binary=request.restic_binary,
                source_repository=request.local_repository,
                source_password_env_var=request.local_password_env_var,
                destination_repository=request.remote_repository,
                destination_password_env_var=request.remote_password_env_var,
                timeout_seconds=request.timeout_seconds,
            )
        )
    except Exception as exc:
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="failed",
            retention_status="skipped",
            archive_error=str(exc),
            archive_exception=exc,
        )

    try:
        retention_result = retention_runner(
            ResticForgetKeepLastRequest(
                keep_last=request.local_retention_keep_last,
                binary=request.restic_binary,
                repository=request.local_repository,
                password_env_var=request.local_password_env_var,
                timeout_seconds=request.timeout_seconds,
            )
        )
    except Exception as exc:
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="completed",
            retention_status="failed",
            archive_result=archive_result,
            retention_error=str(exc),
            retention_exception=exc,
        )

    return PostBackupArchiveResult(
        snapshot_id=request.snapshot_id,
        remote_repository=request.remote_repository,
        local_retention_keep_last=request.local_retention_keep_last,
        archive_status="completed",
        retention_status="completed",
        archive_result=archive_result,
        retention_result=retention_result,
    )
