from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float


class CommandExitError(RuntimeError):
    def __init__(self, result: CommandResult) -> None:
        super().__init__(f"Command exited with return code {result.returncode}: {result.argv!r}")
        self.result = result


class CommandTimeoutError(RuntimeError):
    def __init__(
        self,
        *,
        argv: tuple[str, ...],
        timeout_seconds: float,
        stdout: str | None,
        stderr: str | None,
    ) -> None:
        super().__init__(f"Command timed out after {timeout_seconds} seconds: {argv!r}")
        self.argv = argv
        self.timeout_seconds = timeout_seconds
        self.stdout = stdout
        self.stderr = stderr


def run_command(
    argv: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env_overrides: Mapping[str, str] | None = None,
    timeout_seconds: float | None = None,
    check: bool = False,
) -> CommandResult:
    normalized_argv = tuple(argv)
    env = None
    if env_overrides is not None:
        env = os.environ.copy()
        env.update(env_overrides)

    started_at = perf_counter()
    try:
        completed = subprocess.run(
            normalized_argv,
            cwd=Path(cwd) if cwd is not None else None,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise CommandTimeoutError(
            argv=normalized_argv,
            timeout_seconds=timeout_seconds if timeout_seconds is not None else 0.0,
            stdout=_decode_timeout_output(exc.stdout),
            stderr=_decode_timeout_output(exc.stderr),
        ) from exc

    duration_seconds = perf_counter() - started_at
    result = CommandResult(
        argv=normalized_argv,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_seconds=duration_seconds,
    )
    if check and result.returncode != 0:
        raise CommandExitError(result)
    return result


def _decode_timeout_output(output: str | bytes | None) -> str | None:
    if output is None or isinstance(output, str):
        return output
    return output.decode()
