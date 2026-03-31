from pathlib import Path

import pytest

from backup_projects.adapters.process.command_runner import CommandResult
from backup_projects.adapters.process.restic_runner import (
    ResticCommandFailureError,
    ResticTimeoutError,
)
from backup_projects.adapters.restic_adapter import (
    ResticBackupRequest,
    ResticBackupResult,
    ResticOutputParseError,
    ResticSnapshotIdMissingError,
)
from backup_projects.domain import CandidateFile, FinalDecision, ManifestResult
from backup_projects.services.backup_service import (
    BackupServiceRequest,
    BackupServiceResult,
    run_backup_from_manifest,
)


def test_run_backup_from_manifest_builds_restic_request_and_returns_result(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "backup.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    manifest_result = _make_manifest_result(manifest_file_path=manifest_file_path)
    captured: dict[str, object] = {}
    expected_restic_result = ResticBackupResult(
        manifest_file_path=str(manifest_file_path),
        snapshot_id="snapshot-123",
        summary_payload={
            "message_type": "summary",
            "snapshot_id": "snapshot-123",
            "files_new": 1,
        },
        argv=(
            "restic",
            "backup",
            "--json",
            "--files-from-verbatim",
            str(manifest_file_path),
        ),
        stdout='{"message_type":"summary","snapshot_id":"snapshot-123","files_new":1}',
        stderr="",
        duration_seconds=1.25,
    )

    def fake_backup_runner(request: ResticBackupRequest) -> ResticBackupResult:
        captured["request"] = request
        return expected_restic_result

    result = run_backup_from_manifest(
        BackupServiceRequest(
            manifest_result=manifest_result,
            restic_binary="restic",
            restic_repository="/mnt/backup/repo",
            restic_password_env_var="RESTIC_PASSWORD",
            restic_timeout_seconds=300,
        ),
        backup_runner=fake_backup_runner,
    )

    assert captured["request"] == ResticBackupRequest(
        manifest_file_path=str(manifest_file_path),
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="RESTIC_PASSWORD",
        timeout_seconds=300,
    )
    assert result == BackupServiceResult(
        manifest_result=manifest_result,
        restic_result=expected_restic_result,
    )


def test_run_backup_from_manifest_validates_with_stripped_path_but_passes_original_string(
    tmp_path: Path,
) -> None:
    real_manifest_file_path = tmp_path / "backup.manifest.txt"
    real_manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    stored_manifest_file_path = f"  {real_manifest_file_path}  "
    manifest_result = _make_manifest_result(manifest_file_path=stored_manifest_file_path)
    captured: dict[str, object] = {}
    expected_restic_result = ResticBackupResult(
        manifest_file_path=stored_manifest_file_path,
        snapshot_id="snapshot-123",
        summary_payload={
            "message_type": "summary",
            "snapshot_id": "snapshot-123",
        },
        argv=("restic",),
        stdout="",
        stderr="",
        duration_seconds=1.0,
    )

    def fake_backup_runner(request: ResticBackupRequest) -> ResticBackupResult:
        captured["request"] = request
        return expected_restic_result

    result = run_backup_from_manifest(
        BackupServiceRequest(
            manifest_result=manifest_result,
            restic_binary="restic",
            restic_repository="/mnt/backup/repo",
            restic_password_env_var="RESTIC_PASSWORD",
            restic_timeout_seconds=300,
        ),
        backup_runner=fake_backup_runner,
    )

    assert captured["request"] == ResticBackupRequest(
        manifest_file_path=stored_manifest_file_path,
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="RESTIC_PASSWORD",
        timeout_seconds=300,
    )
    assert result.restic_result is expected_restic_result


def test_run_backup_from_manifest_rejects_blank_manifest_path() -> None:
    with pytest.raises(ValueError, match="manifest_result.manifest_file_path must not be empty"):
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=_make_manifest_result(manifest_file_path="   "),
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=300,
            )
        )


def test_run_backup_from_manifest_rejects_missing_manifest_file(tmp_path: Path) -> None:
    missing_manifest_file_path = tmp_path / "missing.manifest.txt"

    with pytest.raises(FileNotFoundError) as exc_info:
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=_make_manifest_result(
                    manifest_file_path=missing_manifest_file_path
                ),
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=300,
            )
        )

    assert str(exc_info.value) == str(missing_manifest_file_path)


def test_run_backup_from_manifest_rejects_directory_manifest_path(
    tmp_path: Path,
) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()

    with pytest.raises(FileNotFoundError) as exc_info:
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=_make_manifest_result(manifest_file_path=manifest_dir),
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=300,
            )
        )

    assert str(exc_info.value) == str(manifest_dir)


def test_run_backup_from_manifest_skips_empty_manifest_without_invoking_restic(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "empty.manifest.txt"
    manifest_file_path.write_text("", encoding="utf-8")
    captured = {"called": False}

    def fake_backup_runner(_: ResticBackupRequest) -> ResticBackupResult:
        captured["called"] = True
        raise AssertionError("restic runner should not be called for an empty manifest")

    result = run_backup_from_manifest(
        BackupServiceRequest(
            manifest_result=ManifestResult(
                manifest_paths=(),
                decisions=(),
                manifest_file_path=str(manifest_file_path),
                json_manifest_file_path=f"{manifest_file_path}.json",
                summary_file_path=f"{manifest_file_path}.summary",
            ),
            restic_binary="restic",
            restic_repository="/mnt/backup/repo",
            restic_password_env_var="RESTIC_PASSWORD",
            restic_timeout_seconds=300,
        ),
        backup_runner=fake_backup_runner,
    )

    assert captured["called"] is False
    assert result.restic_result is None
    assert result.message == "Backup skipped: manifest include set is empty"


def test_run_backup_from_manifest_propagates_restic_command_failure_unchanged(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "backup.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    request = BackupServiceRequest(
        manifest_result=_make_manifest_result(manifest_file_path=manifest_file_path),
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=300,
    )
    error = ResticCommandFailureError(
        result=CommandResult(
            argv=("restic",),
            returncode=3,
            stdout="",
            stderr="stderr",
            duration_seconds=1.0,
        )
    )

    def fake_backup_runner(_: ResticBackupRequest) -> ResticBackupResult:
        raise error

    with pytest.raises(ResticCommandFailureError) as exc_info:
        run_backup_from_manifest(request, backup_runner=fake_backup_runner)

    assert exc_info.value is error


def test_run_backup_from_manifest_propagates_restic_timeout_unchanged(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "backup.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    request = BackupServiceRequest(
        manifest_result=_make_manifest_result(manifest_file_path=manifest_file_path),
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=300,
    )
    error = ResticTimeoutError(
        argv=("restic", "backup"),
        timeout_seconds=300,
        stdout="partial stdout",
        stderr="partial stderr",
    )

    def fake_backup_runner(_: ResticBackupRequest) -> ResticBackupResult:
        raise error

    with pytest.raises(ResticTimeoutError) as exc_info:
        run_backup_from_manifest(request, backup_runner=fake_backup_runner)

    assert exc_info.value is error


def test_run_backup_from_manifest_propagates_output_parse_error_unchanged(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "backup.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    request = BackupServiceRequest(
        manifest_result=_make_manifest_result(manifest_file_path=manifest_file_path),
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=300,
    )
    error = ResticOutputParseError(
        "bad output",
        stdout="stdout",
        stderr="stderr",
    )

    def fake_backup_runner(_: ResticBackupRequest) -> ResticBackupResult:
        raise error

    with pytest.raises(ResticOutputParseError) as exc_info:
        run_backup_from_manifest(request, backup_runner=fake_backup_runner)

    assert exc_info.value is error


def test_run_backup_from_manifest_propagates_snapshot_id_missing_error_unchanged(
    tmp_path: Path,
) -> None:
    manifest_file_path = tmp_path / "backup.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    request = BackupServiceRequest(
        manifest_result=_make_manifest_result(manifest_file_path=manifest_file_path),
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=300,
    )
    error = ResticSnapshotIdMissingError(
        summary_payload={"message_type": "summary"},
        stdout="stdout",
        stderr="stderr",
    )

    def fake_backup_runner(_: ResticBackupRequest) -> ResticBackupResult:
        raise error

    with pytest.raises(ResticSnapshotIdMissingError) as exc_info:
        run_backup_from_manifest(request, backup_runner=fake_backup_runner)

    assert exc_info.value is error


def _make_manifest_result(manifest_file_path: str | Path) -> ManifestResult:
    candidate = CandidateFile(
        absolute_path="/data/project/file.txt",
        extension="txt",
        size_bytes=10,
        mtime_ns=1,
        ctime_ns=1,
    )
    decision = FinalDecision(
        candidate=candidate,
        include=True,
        reason="policy_include",
    )
    manifest_path = str(manifest_file_path)
    return ManifestResult(
        manifest_paths=(candidate.absolute_path,),
        decisions=(decision,),
        manifest_file_path=manifest_path,
        json_manifest_file_path=f"{manifest_path}.json",
        summary_file_path=f"{manifest_path}.summary",
    )
