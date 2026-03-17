from pathlib import Path
from textwrap import dedent

from sqlalchemy import inspect, text

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    connection_scope,
    create_engine_from_config,
    create_engine_from_db_settings,
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.adapters.db.sqlite_utils import DEFAULT_SQLITE_BUSY_TIMEOUT_MS
from backup_projects.config import DbSettings, load_config

ROOT = Path(__file__).resolve().parents[1]


def test_create_schema_creates_sqlite_file(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "db" / "foundation.sqlite3"
    engine = create_sqlite_engine(db_path)

    create_schema(engine)

    assert db_path.exists()


def test_create_schema_creates_core_tables_and_constraints(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "db" / "schema.sqlite3"
    engine = create_sqlite_engine(db_path)

    create_schema(engine)

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    assert table_names == {
        "excluded_patterns",
        "extension_rules",
        "manual_includes",
        "project_dirs",
        "project_files",
        "roots",
        "run_events",
        "runs",
        "settings",
        "unrecognized_extensions",
    }
    assert "run_artifacts" not in table_names

    project_dirs_foreign_keys = inspector.get_foreign_keys("project_dirs")
    project_files_uniques = inspector.get_unique_constraints("project_files")
    run_events_indexes = inspector.get_indexes("run_events")

    assert project_dirs_foreign_keys == [
        {
            "name": None,
            "constrained_columns": ["root_id"],
            "referred_schema": None,
            "referred_table": "roots",
            "referred_columns": ["id"],
            "options": {"ondelete": "CASCADE"},
        }
    ]
    assert {unique["name"] for unique in project_files_uniques} == {
        "uq_project_files_project_dir_relative_path"
    }
    assert {index["name"] for index in run_events_indexes} >= {
        "ix_run_events_event_type",
        "ix_run_events_level",
        "ix_run_events_run_id_event_time",
    }


def test_sqlite_pragmas_are_applied(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "db" / "pragmas.sqlite3"
    engine = create_sqlite_engine(db_path)

    with connection_scope(engine) as connection:
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()

    assert foreign_keys == 1
    assert busy_timeout == DEFAULT_SQLITE_BUSY_TIMEOUT_MS


def test_session_scope_executes_sync_queries(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "db" / "session.sqlite3"
    engine = create_sqlite_engine(db_path)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        result = session.execute(text("SELECT 1")).scalar_one()

    assert result == 1


def test_create_engine_from_db_settings(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "db" / "from-settings.sqlite3"
    db_settings = DbSettings(driver="sqlite", sqlite_path=str(db_path))

    engine = create_engine_from_db_settings(db_settings)
    create_schema(engine)

    assert db_path.exists()


def test_create_engine_from_loaded_config(tmp_path: Path) -> None:
    relative_db_path = Path("runtime/db/from-config.sqlite3")
    db_path = tmp_path / relative_db_path
    app_path = tmp_path / "app.yaml"

    app_path.write_text(
        dedent(
            f"""
            app:
              name: "backup-projects"
              env: "local"
              log_level: "INFO"
            raid_roots:
              - name: "raid_a"
                path: "/mnt/raid_a/projects"
                enabled: true
            runtime:
              logs_dir: "runtime/logs"
              manifests_dir: "runtime/manifests"
              reports_dir: "runtime/reports"
              db_dir: "runtime/db"
              locks_dir: "runtime/locks"
            web:
              host: "127.0.0.1"
              port: 8080
              debug: false
            db:
              driver: "sqlite"
              sqlite_path: "{relative_db_path.as_posix()}"
            restic:
              binary: "restic"
              repository: "/mnt/backup/restic-repo"
              password_env_var: "RESTIC_PASSWORD"
              timeout_seconds: 7200
            scheduler:
              mode: "cron"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    config_obj = load_config(app_path=app_path, rules_path=ROOT / "config/rules.example.yaml")
    engine = create_engine_from_config(config_obj)
    create_schema(engine)

    assert db_path.exists()
