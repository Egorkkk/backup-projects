import sys

import pytest

from backup_projects.adapters.process.command_runner import (
    CommandExitError,
    CommandResult,
    CommandTimeoutError,
    run_command,
)


def test_run_command_returns_successful_result() -> None:
    result = run_command(
        [
            sys.executable,
            "-c",
            "print('hello'); import sys; print('warn', file=sys.stderr)",
        ]
    )

    assert isinstance(result, CommandResult)
    assert result.argv[0] == sys.executable
    assert result.returncode == 0
    assert result.stdout == "hello\n"
    assert result.stderr == "warn\n"
    assert result.duration_seconds >= 0.0


def test_run_command_returns_nonzero_result_when_check_is_false() -> None:
    result = run_command([sys.executable, "-c", "import sys; sys.exit(7)"])

    assert result.returncode == 7
    assert result.stdout == ""
    assert result.stderr == ""


def test_run_command_raises_command_exit_error_when_check_is_true() -> None:
    with pytest.raises(CommandExitError) as exc_info:
        run_command([sys.executable, "-c", "import sys; sys.exit(3)"], check=True)

    assert exc_info.value.result.returncode == 3
    assert exc_info.value.result.argv == (sys.executable, "-c", "import sys; sys.exit(3)")


def test_run_command_raises_timeout_error() -> None:
    with pytest.raises(CommandTimeoutError) as exc_info:
        run_command(
            [sys.executable, "-c", "import time; print('start'); time.sleep(1)"],
            timeout_seconds=0.05,
        )

    assert exc_info.value.argv[0] == sys.executable
    assert exc_info.value.timeout_seconds == 0.05
    assert exc_info.value.stdout in (None, "", "start\n")


def test_run_command_supports_cwd(tmp_path) -> None:
    result = run_command(
        [sys.executable, "-c", "from pathlib import Path; print(Path.cwd().name)"],
        cwd=tmp_path,
    )

    assert result.stdout.strip() == tmp_path.name


def test_run_command_applies_env_overrides_over_current_environment() -> None:
    result = run_command(
        [
            sys.executable,
            "-c",
            "import os; print(os.environ['BP_TEST_VAR']); print(os.environ.get('PATH', '') != '')",
        ],
        env_overrides={"BP_TEST_VAR": "override-value"},
    )

    stdout_lines = result.stdout.strip().splitlines()
    assert stdout_lines[0] == "override-value"
    assert stdout_lines[1] == "True"


def test_run_command_propagates_file_not_found_error() -> None:
    with pytest.raises(FileNotFoundError):
        run_command(["/definitely/missing/command"])
