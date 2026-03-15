import pytest

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
)
from backup_projects.adapters.process.restic_runner import (
    ResticBackupRequest,
    ResticCommandFailureError,
    ResticTimeoutError,
    run_restic_backup_command,
)


def test_run_restic_backup_command_builds_expected_argv_and_env(monkeypatch) -> None:
    monkeypatch.setenv("BP_RESTIC_PASSWORD", "secret-value")
    captured: dict[str, object] = {}
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="BP_RESTIC_PASSWORD",
        timeout_seconds=120,
    )

    def fake_command_runner(
        argv,
        *,
        cwd=None,
        env_overrides=None,
        timeout_seconds=None,
        check=False,
    ) -> CommandResult:
        captured["argv"] = argv
        captured["cwd"] = cwd
        captured["env_overrides"] = env_overrides
        captured["timeout_seconds"] = timeout_seconds
        captured["check"] = check
        return CommandResult(
            argv=tuple(argv),
            returncode=0,
            stdout="",
            stderr="",
            duration_seconds=0.25,
        )

    result = run_restic_backup_command(request, command_runner=fake_command_runner)

    assert result.returncode == 0
    assert captured["argv"] == (
        "restic",
        "backup",
        "--json",
        "--files-from-verbatim",
        "/tmp/final.manifest.txt",
    )
    assert captured["cwd"] is None
    assert captured["env_overrides"] == {
        "RESTIC_REPOSITORY": "/mnt/backup/repo",
        "RESTIC_PASSWORD": "secret-value",
    }
    assert captured["timeout_seconds"] == 120
    assert captured["check"] is True


def test_run_restic_backup_command_reads_password_from_named_env_var(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CUSTOM_RESTIC_PASSWORD", "named-secret")
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="CUSTOM_RESTIC_PASSWORD",
        timeout_seconds=30,
    )

    def fake_command_runner(
        argv,
        *,
        cwd=None,
        env_overrides=None,
        timeout_seconds=None,
        check=False,
    ) -> CommandResult:
        assert env_overrides is not None
        assert env_overrides["RESTIC_PASSWORD"] == "named-secret"
        return CommandResult(
            argv=tuple(argv),
            returncode=0,
            stdout="ok",
            stderr="",
            duration_seconds=0.1,
        )

    result = run_restic_backup_command(request, command_runner=fake_command_runner)

    assert result.stdout == "ok"


def test_run_restic_backup_command_maps_nonzero_exit_to_restic_error(
    monkeypatch,
) -> None:
    monkeypatch.setenv("BP_RESTIC_PASSWORD", "secret-value")
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
        stdout="",
        stderr="failure",
        duration_seconds=1.5,
    )

    def fake_command_runner(
        argv,
        *,
        cwd=None,
        env_overrides=None,
        timeout_seconds=None,
        check=False,
    ) -> CommandResult:
        raise CommandExitError(failure_result)

    with pytest.raises(ResticCommandFailureError) as exc_info:
        run_restic_backup_command(request, command_runner=fake_command_runner)

    assert exc_info.value.argv == failure_result.argv
    assert exc_info.value.returncode == 3
    assert exc_info.value.stderr == "failure"
    assert exc_info.value.duration_seconds == 1.5


def test_run_restic_backup_command_maps_timeout_to_restic_error(
    monkeypatch,
) -> None:
    monkeypatch.setenv("BP_RESTIC_PASSWORD", "secret-value")
    request = ResticBackupRequest(
        manifest_file_path="/tmp/final.manifest.txt",
        binary="restic",
        repository="/mnt/backup/repo",
        password_env_var="BP_RESTIC_PASSWORD",
        timeout_seconds=90,
    )

    def fake_command_runner(
        argv,
        *,
        cwd=None,
        env_overrides=None,
        timeout_seconds=None,
        check=False,
    ) -> CommandResult:
        raise CommandTimeoutError(
            argv=tuple(argv),
            timeout_seconds=90,
            stdout="partial stdout",
            stderr="partial stderr",
        )

    with pytest.raises(ResticTimeoutError) as exc_info:
        run_restic_backup_command(request, command_runner=fake_command_runner)

    assert exc_info.value.argv == (
        "restic",
        "backup",
        "--json",
        "--files-from-verbatim",
        "/tmp/final.manifest.txt",
    )
    assert exc_info.value.timeout_seconds == 90
    assert exc_info.value.stdout == "partial stdout"
    assert exc_info.value.stderr == "partial stderr"
