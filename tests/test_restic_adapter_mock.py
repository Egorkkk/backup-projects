from pathlib import Path

import pytest

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
)
from backup_projects.adapters.restic_adapter import (
    ResticCommandFailureError,
    ResticOutputParseError,
    ResticTimeoutError,
    run_restic_backup,
)
from backup_projects.domain import ManifestResult
from backup_projects.services.backup_service import (
    BackupServiceRequest,
    run_backup_from_manifest,
)


def test_run_backup_from_manifest_composes_service_and_adapter_with_mocked_runner(
    tmp_path: Path,
) -> None:
    manifest_result = _make_manifest_result(tmp_path)

    def fake_low_level_runner(request) -> CommandResult:
        assert request.manifest_file_path == manifest_result.manifest_file_path
        return CommandResult(
            argv=(
                "restic",
                "backup",
                "--json",
                "--files-from-verbatim",
                manifest_result.manifest_file_path,
            ),
            returncode=0,
            stdout="\n".join(
                [
                    '{"message_type":"status","files_done":1}',
                    '{"message_type":"summary","snapshot_id":"snapshot-123","files_new":3}',
                ]
            ),
            stderr="warning text",
            duration_seconds=2.5,
        )

    result = run_backup_from_manifest(
        BackupServiceRequest(
            manifest_result=manifest_result,
            restic_binary="restic",
            restic_repository="/mnt/backup/repo",
            restic_password_env_var="RESTIC_PASSWORD",
            restic_timeout_seconds=7200,
        ),
        backup_runner=lambda request: run_restic_backup(
            request,
            runner=fake_low_level_runner,
        ),
    )

    assert result.manifest_result is manifest_result
    assert result.restic_result.snapshot_id == "snapshot-123"
    assert result.restic_result.stdout == "\n".join(
        [
            '{"message_type":"status","files_done":1}',
            '{"message_type":"summary","snapshot_id":"snapshot-123","files_new":3}',
        ]
    )
    assert result.restic_result.stderr == "warning text"
    assert result.restic_result.argv == (
        "restic",
        "backup",
        "--json",
        "--files-from-verbatim",
        manifest_result.manifest_file_path,
    )
    assert result.restic_result.duration_seconds == 2.5


def test_run_backup_from_manifest_maps_command_failure_from_mocked_runner(
    tmp_path: Path,
) -> None:
    manifest_result = _make_manifest_result(tmp_path)
    command_result = CommandResult(
        argv=("restic", "backup"),
        returncode=3,
        stdout="failure stdout",
        stderr="failure stderr",
        duration_seconds=1.25,
    )

    def fake_low_level_runner(request) -> CommandResult:
        raise CommandExitError(command_result)

    with pytest.raises(ResticCommandFailureError) as exc_info:
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=manifest_result,
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=7200,
            ),
            backup_runner=lambda request: run_restic_backup(
                request,
                runner=fake_low_level_runner,
            ),
        )

    assert exc_info.value.argv == ("restic", "backup")
    assert exc_info.value.returncode == 3
    assert exc_info.value.stdout == "failure stdout"
    assert exc_info.value.stderr == "failure stderr"


def test_run_backup_from_manifest_maps_timeout_from_mocked_runner(
    tmp_path: Path,
) -> None:
    manifest_result = _make_manifest_result(tmp_path)

    def fake_low_level_runner(request) -> CommandResult:
        raise CommandTimeoutError(
            argv=("restic", "backup"),
            timeout_seconds=7200,
            stdout="partial stdout",
            stderr="partial stderr",
        )

    with pytest.raises(ResticTimeoutError) as exc_info:
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=manifest_result,
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=7200,
            ),
            backup_runner=lambda request: run_restic_backup(
                request,
                runner=fake_low_level_runner,
            ),
        )

    assert exc_info.value.argv == ("restic", "backup")
    assert exc_info.value.timeout_seconds == 7200
    assert exc_info.value.stdout == "partial stdout"
    assert exc_info.value.stderr == "partial stderr"


def test_run_backup_from_manifest_maps_malformed_output_from_mocked_runner(
    tmp_path: Path,
) -> None:
    manifest_result = _make_manifest_result(tmp_path)

    def fake_low_level_runner(request) -> CommandResult:
        return CommandResult(
            argv=("restic", "backup"),
            returncode=0,
            stdout="not-json",
            stderr="warning text",
            duration_seconds=1.0,
        )

    with pytest.raises(ResticOutputParseError) as exc_info:
        run_backup_from_manifest(
            BackupServiceRequest(
                manifest_result=manifest_result,
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=7200,
            ),
            backup_runner=lambda request: run_restic_backup(
                request,
                runner=fake_low_level_runner,
            ),
        )

    assert exc_info.value.stdout == "not-json"
    assert exc_info.value.stderr == "warning text"


def _make_manifest_result(tmp_path: Path) -> ManifestResult:
    manifest_file_path = tmp_path / "daily.manifest.txt"
    manifest_file_path.write_text("/data/project/file.txt\n", encoding="utf-8")
    return ManifestResult(
        manifest_paths=("/data/project/file.txt",),
        decisions=(),
        manifest_file_path=str(manifest_file_path),
        json_manifest_file_path=str(tmp_path / "daily.manifest.json"),
        summary_file_path=str(tmp_path / "daily.summary.txt"),
    )
