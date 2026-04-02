from backup_projects.adapters.restic_adapter import (
    ResticCommandFailureError,
    ResticCopySnapshotResult,
    ResticForgetKeepLastResult,
)
from backup_projects.adapters.process.command_runner import CommandResult
from backup_projects.services.post_backup_archive_service import (
    PostBackupArchiveRequest,
    run_post_backup_archive,
)


def test_run_post_backup_archive_success_runs_archive_then_retention() -> None:
    request = _make_request()
    call_order: list[str] = []

    def fake_archive_runner(copy_request):
        call_order.append(f"archive:{copy_request.snapshot_id}")
        return ResticCopySnapshotResult(
            snapshot_id=copy_request.snapshot_id,
            argv=("restic", "copy", copy_request.snapshot_id),
            stdout="copied",
            stderr="",
            duration_seconds=1.0,
        )

    def fake_retention_runner(retention_request):
        call_order.append(f"retention:{retention_request.keep_last}")
        return ResticForgetKeepLastResult(
            keep_last=retention_request.keep_last,
            argv=("restic", "forget", "--keep-last", str(retention_request.keep_last)),
            stdout="forgot",
            stderr="",
            duration_seconds=2.0,
        )

    result = run_post_backup_archive(
        request,
        archive_runner=fake_archive_runner,
        retention_runner=fake_retention_runner,
    )

    assert call_order == ["archive:snapshot-123", "retention:1"]
    assert result.archive_status == "completed"
    assert result.retention_status == "completed"
    assert result.archive_result is not None
    assert result.retention_result is not None
    assert result.archive_error is None
    assert result.retention_error is None


def test_run_post_backup_archive_skips_retention_when_archive_fails() -> None:
    request = _make_request()
    call_order: list[str] = []
    archive_failure = ResticCommandFailureError(
        CommandResult(
            argv=("restic", "copy", "snapshot-123"),
            returncode=3,
            stdout="",
            stderr="copy failed",
            duration_seconds=1.5,
        )
    )

    def fake_archive_runner(copy_request):
        call_order.append(f"archive:{copy_request.snapshot_id}")
        raise archive_failure

    def fake_retention_runner(_retention_request):
        call_order.append("retention")
        raise AssertionError("retention must not run when archive fails")

    result = run_post_backup_archive(
        request,
        archive_runner=fake_archive_runner,
        retention_runner=fake_retention_runner,
    )

    assert call_order == ["archive:snapshot-123"]
    assert result.archive_status == "failed"
    assert result.retention_status == "skipped"
    assert result.archive_result is None
    assert result.retention_result is None
    assert result.archive_error == str(archive_failure)
    assert result.archive_exception is archive_failure


def test_run_post_backup_archive_reports_retention_failure_distinctly() -> None:
    request = _make_request()
    call_order: list[str] = []
    retention_failure = ResticCommandFailureError(
        CommandResult(
            argv=("restic", "forget", "--keep-last", "1"),
            returncode=10,
            stdout="",
            stderr="prune failed",
            duration_seconds=2.5,
        )
    )

    def fake_archive_runner(copy_request):
        call_order.append(f"archive:{copy_request.snapshot_id}")
        return ResticCopySnapshotResult(
            snapshot_id=copy_request.snapshot_id,
            argv=("restic", "copy", copy_request.snapshot_id),
            stdout="copied",
            stderr="",
            duration_seconds=1.0,
        )

    def fake_retention_runner(retention_request):
        call_order.append(f"retention:{retention_request.keep_last}")
        raise retention_failure

    result = run_post_backup_archive(
        request,
        archive_runner=fake_archive_runner,
        retention_runner=fake_retention_runner,
    )

    assert call_order == ["archive:snapshot-123", "retention:1"]
    assert result.archive_status == "completed"
    assert result.retention_status == "failed"
    assert result.archive_result is not None
    assert result.retention_result is None
    assert result.retention_error == str(retention_failure)
    assert result.retention_exception is retention_failure


def _make_request() -> PostBackupArchiveRequest:
    return PostBackupArchiveRequest(
        snapshot_id="snapshot-123",
        restic_binary="restic",
        local_repository="/mnt/backup/local-repo",
        local_password_env_var="LOCAL_RESTIC_PASSWORD",
        remote_repository="/mnt/backup/remote-repo",
        remote_password_env_var="REMOTE_RESTIC_PASSWORD",
        timeout_seconds=600,
        local_retention_keep_last=1,
    )
