from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path


_MANAGED_HANDLER_ATTR = "_backup_projects_managed_handler"


@dataclass(frozen=True, slots=True)
class RunLoggingConfig:
    run_id: int
    logs_dir: str | Path
    logger_name: str
    console_level: str | int
    file_level: str | int


@dataclass(frozen=True, slots=True)
class RunLoggingContext:
    logger: logging.Logger
    log_dir: Path
    log_file_path: Path


def build_run_log_path(*, logs_dir: str | Path, run_id: int) -> Path:
    return Path(logs_dir) / f"run-{run_id}" / "run.log"


def configure_run_logging(config: RunLoggingConfig) -> RunLoggingContext:
    log_file_path = build_run_log_path(logs_dir=config.logs_dir, run_id=config.run_id)
    log_dir = log_file_path.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(config.logger_name)
    _remove_managed_handlers(logger)
    logger.setLevel(
        min(
            _coerce_log_level(config.console_level),
            _coerce_log_level(config.file_level),
        )
    )
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(_coerce_log_level(config.console_level))
    console_handler.setFormatter(formatter)
    setattr(console_handler, _MANAGED_HANDLER_ATTR, True)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(_coerce_log_level(config.file_level))
    file_handler.setFormatter(formatter)
    setattr(file_handler, _MANAGED_HANDLER_ATTR, True)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return RunLoggingContext(
        logger=logger,
        log_dir=log_dir,
        log_file_path=log_file_path,
    )


def _remove_managed_handlers(logger: logging.Logger) -> None:
    for handler in tuple(logger.handlers):
        if not getattr(handler, _MANAGED_HANDLER_ATTR, False):
            continue
        logger.removeHandler(handler)
        handler.close()


def _coerce_log_level(level: str | int) -> int:
    if isinstance(level, bool):
        raise ValueError(f"Invalid log level: {level!r}")
    if isinstance(level, int):
        return level

    normalized = level.strip().upper()
    if normalized == "":
        raise ValueError("Log level must not be empty")

    resolved_level = logging.getLevelName(normalized)
    if isinstance(resolved_level, int):
        return resolved_level
    raise ValueError(f"Invalid log level: {level!r}")
