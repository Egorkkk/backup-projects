from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase

metadata = MetaData()


class Base(DeclarativeBase):
    metadata = metadata


roots = Table(
    "roots",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("raid_name", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("path", Text, nullable=False),
    Column("device_id", Integer),
    Column("inode", Integer),
    Column("mtime_ns", Integer),
    Column("ctime_ns", Integer),
    Column("is_missing", Boolean, nullable=False, default=False),
    Column("needs_structural_rescan", Boolean, nullable=False, default=False),
    Column("first_seen_at", Text, nullable=False),
    Column("last_seen_at", Text, nullable=False),
    UniqueConstraint("path", name="uq_roots_path"),
    Index("ix_roots_raid_name", "raid_name"),
    Index("ix_roots_is_missing", "is_missing"),
    Index("ix_roots_needs_structural_rescan", "needs_structural_rescan"),
)

project_dirs = Table(
    "project_dirs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "root_id",
        ForeignKey("roots.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("relative_path", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("dir_type", Text, nullable=False),
    Column("is_missing", Boolean, nullable=False, default=False),
    Column("first_seen_at", Text, nullable=False),
    Column("last_seen_at", Text, nullable=False),
    UniqueConstraint("root_id", "relative_path", name="uq_project_dirs_root_relative_path"),
    Index("ix_project_dirs_root_id", "root_id"),
    Index("ix_project_dirs_dir_type", "dir_type"),
    Index("ix_project_dirs_is_missing", "is_missing"),
)

project_files = Table(
    "project_files",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "project_dir_id",
        ForeignKey("project_dirs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("relative_path", Text, nullable=False),
    Column("filename", Text, nullable=False),
    Column("extension", Text, nullable=False),
    Column("size_bytes", Integer, nullable=False),
    Column("mtime_ns", Integer, nullable=False),
    Column("ctime_ns", Integer, nullable=False),
    Column("inode", Integer),
    Column("is_missing", Boolean, nullable=False, default=False),
    Column("first_seen_at", Text, nullable=False),
    Column("last_seen_at", Text, nullable=False),
    UniqueConstraint(
        "project_dir_id",
        "relative_path",
        name="uq_project_files_project_dir_relative_path",
    ),
    Index("ix_project_files_project_dir_id", "project_dir_id"),
    Index("ix_project_files_extension", "extension"),
    Index("ix_project_files_is_missing", "is_missing"),
    Index("ix_project_files_project_dir_extension", "project_dir_id", "extension"),
)

manual_includes = Table(
    "manual_includes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "root_id",
        ForeignKey("roots.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("relative_path", Text, nullable=False),
    Column("include_path_type", Text, nullable=False),
    Column("recursive", Boolean, nullable=False, default=False, server_default=text("0")),
    Column("force_include", Boolean, nullable=False, default=False, server_default=text("0")),
    Column("enabled", Boolean, nullable=False, default=True, server_default=text("1")),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    UniqueConstraint(
        "root_id",
        "relative_path",
        name="uq_manual_includes_root_relative_path",
    ),
    Index("ix_manual_includes_root_id", "root_id"),
    Index("ix_manual_includes_root_id_enabled", "root_id", "enabled"),
)

extension_rules = Table(
    "extension_rules",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("extension", Text, nullable=False),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("max_size_bytes", Integer),
    Column("oversize_action", Text, nullable=False),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    UniqueConstraint("extension", name="uq_extension_rules_extension"),
    Index("ix_extension_rules_enabled", "enabled"),
)

excluded_patterns = Table(
    "excluded_patterns",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("pattern_type", Text, nullable=False),
    Column("pattern_value", Text, nullable=False),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("created_at", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    UniqueConstraint(
        "pattern_type",
        "pattern_value",
        name="uq_excluded_patterns_type_value",
    ),
    Index("ix_excluded_patterns_enabled", "enabled"),
    Index("ix_excluded_patterns_pattern_type", "pattern_type"),
)

settings = Table(
    "settings",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("key", Text, nullable=False),
    Column("value_json", Text, nullable=False),
    Column("updated_at", Text, nullable=False),
    UniqueConstraint("key", name="uq_settings_key"),
    Index("ix_settings_key", "key"),
)

runs = Table(
    "runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("run_type", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("started_at", Text, nullable=False),
    Column("finished_at", Text),
    Column("trigger_mode", Text, nullable=False),
    Index("ix_runs_run_type", "run_type"),
    Index("ix_runs_status", "status"),
    Index("ix_runs_started_at", "started_at"),
)

run_events = Table(
    "run_events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "run_id",
        ForeignKey("runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("event_time", Text, nullable=False),
    Column("level", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("message", Text, nullable=False),
    Column("payload_json", Text),
    Index("ix_run_events_run_id_event_time", "run_id", "event_time"),
    Index("ix_run_events_level", "level"),
    Index("ix_run_events_event_type", "event_type"),
)

unrecognized_extensions = Table(
    "unrecognized_extensions",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("extension", Text, nullable=False),
    Column("sample_path", Text, nullable=False),
    Column("occurrence_count", Integer, nullable=False),
    Column("first_seen_at", Text, nullable=False),
    Column("last_seen_at", Text, nullable=False),
    Column("is_resolved", Boolean, nullable=False, default=False),
    UniqueConstraint("extension", name="uq_unrecognized_extensions_extension"),
    Index("ix_unrecognized_extensions_is_resolved", "is_resolved"),
    Index("ix_unrecognized_extensions_last_seen_at", "last_seen_at"),
)


def create_schema(engine: Engine) -> None:
    with engine.begin() as connection:
        metadata.create_all(bind=connection)


def drop_schema(engine: Engine) -> None:
    with engine.begin() as connection:
        metadata.drop_all(bind=connection)
