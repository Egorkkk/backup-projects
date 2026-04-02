import pytest

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
)
from backup_projects.adapters.restic_adapter import (
    ResticBackupRequest,
    ResticBackupResult,
    ResticCommandFailureError,
    ResticCopySnapshotRequest,
    ResticCopySnapshotResult,
    ResticForgetKeepLastRequest,
    ResticForgetKeepLastResult,
    ResticOutputParseError,
    ResticSnapshotIdMissingError,
    ResticTimeoutError,
    parse_restic_output,
    run_restic_backup,
    run_restic_copy_snapshot,
    run_restic_forget_keep_last_global,
)


def test_parse_restic_output_returns_last_summary_object() -> None:
    stdout = "\n".join(
        [
            '{"message_type":"status","files_done":1}',
            '{"message_type":"summary","snapshot_id":"first","files_new":2}',
            '{"message_type":"summary","snapshot_id":"second","files_new":3}',
        ]
    )

    result = parse_restic_output(stdout=stdout, stderr="warning")

    assert result.snapshot_id == "second"
    assert result.summary_payload == {
        "message_type": "summary",
        "snapshot_id": "second",
        "files_new": 3,
    }


def test_parse_restic_output_tolerates_blank_lines_between_json_lines() -> None:
    stdout = "\n".join(
        [
            '{"message_type":"status","files_done":1}',
            "",
            '{"message_type":"summary","snapshot_id":"abc123","files_new":3}',
            "",
        ]
    )

    result = parse_restic_output(stdout=stdout, stderr="warning")

    assert result.snapshot_id == "abc123"
    assert result.summary_payload == {
        "message_type": "summary",
        "snapshot_id": "abc123",
        "files_new": 3,
    }


def test_parse_restic_output_ignores_valid_non_summary_objects_until_final_summary() -> None:
    stdout = "\n".join(
        [
            '{"message_type":"status","files_done":1}',
            '{"message_type":"verbose_status","current_files":["a.txt"]}',
            '{"message_type":"summary","snapshot_id":"abc123","files_new":3}',
        ]
    )

    result = parse_restic_output(stdout=stdout, stderr="warning")

    assert result.snapshot_id == "abc123"
    assert result.summary_payload == {
        "message_type": "summary",
        "snapshot_id": "abc123",
        "files_new": 3,
    }


def test_parse_restic_output_raises_for_malformed_json() -> None:
    with pytest.raises(ResticOutputParseError) as exc_info:
        parse_restic_output(
            stdout='{"message_type":"status"}\nnot-json',
            stderr="stderr-text",
        )

    assert exc_info.value.stdout == '{"message_type":"status"}\nnot-json'
    assert exc_info.value.stderr == "stderr-text"


def test_parse_restic_output_raises_when_summary_is_missing() -> None:
    with pytest.raises(ResticOutputParseError) as exc_info:
        parse_restic_output(
            stdout='{"message_type":"status","files_done":1}',
            stderr="stderr-text",
        )

    assert exc_info.value.stdout == '{"message_type":"status","files_done":1}'
    assert exc_info.value.stderr == "stderr-text"


def test_parse_restic_output_raises_when_snapshot_id_is_missing() -> None:
    stdout = '{"message_type":"summary","files_new":3}'

    with pytest.raises(ResticSnapshotIdMissingError) as exc_info:
        parse_restic_output(stdout=stdout, stderr="stderr-text")

    assert exc_info.value.summary_payload == {
        "message_type": "summary",
        "files_new": 3,
    }
    assert exc_info.value.stdout == stdout
    assert exc_info.value.stderr == "stderr-text"


def test_parse_restic_output_raises_when_snapshot_id_is_empty() -> None:
    stdout = '{"message_type":"summary","snapshot_id":"   ","files_new":3}'

    with pytest.raises(ResticSnapshotIdMissingError) as exc_info:
        parse_restic_output(stdout=stdout, stderr="stderr-text")

    assert exc_info.value.summary_payload == {
        "message_type": "summary",
        "snapshot_id": "   ",
        "files_new": 3,
    }


def test_run_restic_backup_returns_adapter_result_without_leaking_command_result() -> None:
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="BP_RESTIC_PASSWORD",
        timeout_seconds=120,
    )

    def fake_runner(_: ResticBackupRequest) -> CommandResult:
        return CommandResult(
            argv=(
                "restic",
                "backup",
                "--json",
                "--files-from-verbatim",
                "/tmp/final.manifest.txt",
            ),
            returncode=0,
            stdout="\n".join(
                [
                    '{"message_type":"status","files_done":1}',
                    '{"message_type":"summary","snapshot_id":"abc123","files_new":3}',
                ]
            ),
            stderr="warning text",
            duration_seconds=2.5,
        )

    result = run_restic_backup(request, runner=fake_runner)

    assert isinstance(result, ResticBackupResult)
    assert not isinstance(result, CommandResult)
    assert result == ResticBackupResult(
        manifest_file_path="/tmp/final.manifest.txt",
        snapshot_id="abc123",
        summary_payload={
            "message_type": "summary",
            "snapshot_id": "abc123",
            "files_new": 3,
        },
        argv=(
            "restic",
            "backup",
            "--json",
            "--files-from-verbatim",
            "/tmp/final.manifest.txt",
        ),
        stdout="\n".join(
            [
                '{"message_type":"status","files_done":1}',
                '{"message_type":"summary","snapshot_id":"abc123","files_new":3}',
            ]
        ),
        stderr="warning text",
        duration_seconds=2.5,
    )


def test_run_restic_backup_maps_generic_command_exit_error() -> None:
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="BP_RESTIC_PASSWORD",
        timeout_seconds=120,
    )
    failure_result = CommandResult(
        argv=(
            "restic",
            "backup",
            "--json",
            "--files-from-verbatim",
            "/tmp/final.manifest.txt",
        ),
        returncode=3,
        stdout="failure stdout",
        stderr="failure stderr",
        duration_seconds=1.25,
    )

    def fake_runner(_: ResticBackupRequest) -> CommandResult:
        raise CommandExitError(failure_result)

    with pytest.raises(ResticCommandFailureError) as exc_info:
        run_restic_backup(request, runner=fake_runner)

    assert exc_info.value.argv == failure_result.argv
    assert exc_info.value.returncode == 3
    assert exc_info.value.stdout == "failure stdout"
    assert exc_info.value.stderr == "failure stderr"
    assert exc_info.value.duration_seconds == 1.25


def test_run_restic_backup_maps_generic_command_timeout_error() -> None:
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="BP_RESTIC_PASSWORD",
        timeout_seconds=120,
    )

    def fake_runner(_: ResticBackupRequest) -> CommandResult:
        raise CommandTimeoutError(
            argv=(
                "restic",
                "backup",
                "--json",
                "--files-from-verbatim",
                "/tmp/final.manifest.txt",
            ),
            timeout_seconds=120,
            stdout="partial stdout",
            stderr="partial stderr",
        )

    with pytest.raises(ResticTimeoutError) as exc_info:
        run_restic_backup(request, runner=fake_runner)

    assert exc_info.value.argv == (
        "restic",
        "backup",
        "--json",
        "--files-from-verbatim",
        "/tmp/final.manifest.txt",
    )
    assert exc_info.value.timeout_seconds == 120
    assert exc_info.value.stdout == "partial stdout"
    assert exc_info.value.stderr == "partial stderr"


def test_run_restic_copy_snapshot_returns_adapter_result_without_leaking_command_result() -> None:
    request = ResticCopySnapshotRequest(
        snapshot_id="abc12345",
        binary="restic",
        source_repository="/mnt/backup/local-repo",
        source_password_env_var="LOCAL_RESTIC_PASSWORD",
        destination_repository="/mnt/backup/remote-repo",
        destination_password_env_var="REMOTE_RESTIC_PASSWORD",
        timeout_seconds=600,
    )

    def fake_runner(_: ResticCopySnapshotRequest) -> CommandResult:
        return CommandResult(
            argv=("restic", "copy", "abc12345"),
            returncode=0,
            stdout="copied snapshot abc12345",
            stderr="copy warning",
            duration_seconds=4.5,
        )

    result = run_restic_copy_snapshot(request, runner=fake_runner)

    assert isinstance(result, ResticCopySnapshotResult)
    assert not isinstance(result, CommandResult)
    assert result == ResticCopySnapshotResult(
        snapshot_id="abc12345",
        argv=("restic", "copy", "abc12345"),
        stdout="copied snapshot abc12345",
        stderr="copy warning",
        duration_seconds=4.5,
    )


def test_run_restic_forget_keep_last_global_returns_adapter_result_without_leaking_command_result(
) -> None:
    request = ResticForgetKeepLastRequest(
        keep_last=1,
        binary="restic",
        repository="/mnt/backup/local-repo",
        password_env_var="LOCAL_RESTIC_PASSWORD",
        timeout_seconds=900,
    )

    def fake_runner(_: ResticForgetKeepLastRequest) -> CommandResult:
        return CommandResult(
            argv=(
                "restic",
                "forget",
                "--keep-last",
                "1",
                "--group-by",
                "",
                "--prune",
            ),
            returncode=0,
            stdout="removed 3 snapshots",
            stderr="prune warning",
            duration_seconds=6.0,
        )

    result = run_restic_forget_keep_last_global(request, runner=fake_runner)

    assert isinstance(result, ResticForgetKeepLastResult)
    assert not isinstance(result, CommandResult)
    assert result == ResticForgetKeepLastResult(
        keep_last=1,
        argv=(
            "restic",
            "forget",
            "--keep-last",
            "1",
            "--group-by",
            "",
            "--prune",
        ),
        stdout="removed 3 snapshots",
        stderr="prune warning",
        duration_seconds=6.0,
    )
