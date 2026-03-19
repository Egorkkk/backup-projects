import logging
from pathlib import Path

from backup_projects.services.logging_setup import (
    RunLoggingConfig,
    build_run_log_path,
    configure_run_logging,
)


def test_build_run_log_path_uses_deterministic_run_layout(tmp_path: Path) -> None:
    log_path = build_run_log_path(logs_dir=tmp_path / "logs", run_id=42)

    assert log_path == tmp_path / "logs" / "run-42" / "run.log"


def test_configure_run_logging_creates_run_directory_and_returns_context(
    tmp_path: Path,
) -> None:
    context = configure_run_logging(
        RunLoggingConfig(
            run_id=42,
            logs_dir=tmp_path / "logs",
            logger_name="backup-projects.test.run-42",
            console_level="INFO",
            file_level="DEBUG",
        )
    )

    assert context.log_dir == tmp_path / "logs" / "run-42"
    assert context.log_dir.is_dir()
    assert context.log_file_path == tmp_path / "logs" / "run-42" / "run.log"
    assert context.logger.name == "backup-projects.test.run-42"


def test_configure_run_logging_writes_messages_to_run_log_file(tmp_path: Path) -> None:
    context = configure_run_logging(
        RunLoggingConfig(
            run_id=7,
            logs_dir=tmp_path / "logs",
            logger_name="backup-projects.test.file-write",
            console_level="WARNING",
            file_level="INFO",
        )
    )

    context.logger.info("hello run log")

    log_contents = context.log_file_path.read_text(encoding="utf-8")
    assert "INFO" in log_contents
    assert "backup-projects.test.file-write" in log_contents
    assert "hello run log" in log_contents


def test_configure_run_logging_adds_console_and_file_handlers(tmp_path: Path) -> None:
    context = configure_run_logging(
        RunLoggingConfig(
            run_id=9,
            logs_dir=tmp_path / "logs",
            logger_name="backup-projects.test.handlers",
            console_level="INFO",
            file_level="DEBUG",
        )
    )

    handlers = context.logger.handlers
    assert len(handlers) == 2
    assert sum(type(handler) is logging.StreamHandler for handler in handlers) == 1
    assert sum(isinstance(handler, logging.FileHandler) for handler in handlers) == 1


def test_configure_run_logging_does_not_duplicate_managed_handlers_for_same_logger(
    tmp_path: Path,
) -> None:
    config = RunLoggingConfig(
        run_id=13,
        logs_dir=tmp_path / "logs",
        logger_name="backup-projects.test.repeat",
        console_level="INFO",
        file_level="INFO",
    )

    first_context = configure_run_logging(config)
    first_context.logger.info("first message")
    second_context = configure_run_logging(config)
    second_context.logger.info("second message")

    handlers = second_context.logger.handlers
    log_contents = second_context.log_file_path.read_text(encoding="utf-8")

    assert len(handlers) == 2
    assert sum(type(handler) is logging.StreamHandler for handler in handlers) == 1
    assert sum(isinstance(handler, logging.FileHandler) for handler in handlers) == 1
    assert log_contents.count("second message") == 1
