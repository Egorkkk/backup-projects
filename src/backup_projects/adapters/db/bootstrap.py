from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import Connection
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from backup_projects.adapters.db.schema import (
    create_schema,
    excluded_patterns,
    extension_rules,
    settings,
)
from backup_projects.adapters.db.session import create_engine_from_config
from backup_projects.config import ProjectConfig
from backup_projects.constants import AAF_EXTENSION


def initialize_database(config: ProjectConfig) -> None:
    engine = create_engine_from_config(config)
    try:
        create_schema(engine)

        now_iso = _utcnow_iso()
        with engine.begin() as connection:
            seed_settings(connection, config, now_iso=now_iso)
            seed_extension_rules(connection, config, now_iso=now_iso)
            seed_excluded_patterns(connection, config, now_iso=now_iso)
    finally:
        engine.dispose()


def seed_settings(connection: Connection, config: ProjectConfig, *, now_iso: str) -> None:
    rows = [
        {
            "key": "scheduler.mode",
            "value_json": json.dumps(config.app_config.scheduler.mode.value),
            "updated_at": now_iso,
        },
        {
            "key": "oversize.default_action",
            "value_json": json.dumps(config.rules_config.oversize.default_action.value),
            "updated_at": now_iso,
        },
        {
            "key": "oversize.log_skipped",
            "value_json": json.dumps(config.rules_config.oversize.log_skipped),
            "updated_at": now_iso,
        },
        {
            "key": "unknown_extensions.action",
            "value_json": json.dumps(config.rules_config.unknown_extensions.action.value),
            "updated_at": now_iso,
        },
        {
            "key": "unknown_extensions.store_in_registry",
            "value_json": json.dumps(config.rules_config.unknown_extensions.store_in_registry),
            "updated_at": now_iso,
        },
        {
            "key": "unknown_extensions.log_warning",
            "value_json": json.dumps(config.rules_config.unknown_extensions.log_warning),
            "updated_at": now_iso,
        },
    ]
    statement = sqlite_insert(settings).values(rows)
    statement = statement.on_conflict_do_nothing(index_elements=["key"])
    connection.execute(statement)


def seed_extension_rules(connection: Connection, config: ProjectConfig, *, now_iso: str) -> None:
    limits_by_extension = {
        _normalize_extension(extension): limit
        for extension, limit in config.rules_config.size_limits.by_extension.items()
    }
    default_limit = config.rules_config.size_limits.default_max_size_bytes

    rows = []
    for extension in config.rules_config.allowed_extensions:
        rows.append(
            {
                "extension": extension,
                "enabled": True,
                "max_size_bytes": limits_by_extension.get(extension, default_limit),
                "oversize_action": (
                    config.rules_config.oversize.aaf_action.value
                    if extension == AAF_EXTENSION
                    else config.rules_config.oversize.default_action.value
                ),
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )

    statement = sqlite_insert(extension_rules).values(rows)
    statement = statement.on_conflict_do_nothing(index_elements=["extension"])
    connection.execute(statement)


def seed_excluded_patterns(connection: Connection, config: ProjectConfig, *, now_iso: str) -> None:
    rows = [
        *_pattern_rows(
            "directory_name",
            config.rules_config.exclude_patterns.directory_names,
            now_iso,
        ),
        *_pattern_rows("glob", config.rules_config.exclude_patterns.glob_patterns, now_iso),
        *_pattern_rows(
            "path_substring",
            config.rules_config.exclude_patterns.path_substrings,
            now_iso,
        ),
    ]
    if not rows:
        return

    statement = sqlite_insert(excluded_patterns).values(rows)
    statement = statement.on_conflict_do_nothing(index_elements=["pattern_type", "pattern_value"])
    connection.execute(statement)


def _pattern_rows(pattern_type: str, values: list[str], now_iso: str) -> list[dict[str, object]]:
    return [
        {
            "pattern_type": pattern_type,
            "pattern_value": value,
            "enabled": True,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        for value in values
    ]


def _normalize_extension(extension: str) -> str:
    normalized = extension.strip().lower()
    if normalized.startswith("."):
        normalized = normalized[1:]
    return normalized


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
