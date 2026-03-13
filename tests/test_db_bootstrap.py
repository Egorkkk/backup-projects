import json
from pathlib import Path
from textwrap import dedent

from sqlalchemy import text

from backup_projects.adapters.db.bootstrap import initialize_database
from backup_projects.adapters.db.session import create_engine_from_config
from backup_projects.config import load_config


def test_initialize_database_creates_schema_and_seeds_defaults(tmp_path: Path) -> None:
    config = _load_test_config(tmp_path)

    initialize_database(config)

    engine = create_engine_from_config(config)
    try:
        with engine.begin() as connection:
            settings_rows = connection.execute(
                text("SELECT key, value_json FROM settings ORDER BY key")
            ).all()
            extension_rows = connection.execute(
                text(
                    "SELECT extension, enabled, max_size_bytes, oversize_action "
                    "FROM extension_rules ORDER BY extension"
                )
            ).all()
            excluded_rows = connection.execute(
                text(
                    "SELECT pattern_type, pattern_value, enabled "
                    "FROM excluded_patterns ORDER BY pattern_type, pattern_value"
                )
            ).all()
    finally:
        engine.dispose()

    assert {key: json.loads(value_json) for key, value_json in settings_rows} == {
        "oversize.default_action": "warn",
        "oversize.log_skipped": True,
        "scheduler.mode": "cron",
        "unknown_extensions.action": "collect_and_skip",
        "unknown_extensions.log_warning": True,
        "unknown_extensions.store_in_registry": True,
    }
    assert extension_rows == [
        ("aaf", 1, 104857600, "skip"),
        ("prproj", 1, None, "warn"),
    ]
    assert excluded_rows == [
        ("directory_name", "Cache", 1),
        ("glob", "**/.cache/**", 1),
        ("path_substring", "Autosave", 1),
    ]


def test_initialize_database_is_rerunnable_and_preserves_existing_values(
    tmp_path: Path,
) -> None:
    config = _load_test_config(tmp_path)

    initialize_database(config)

    engine = create_engine_from_config(config)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE settings SET value_json = :value_json WHERE key = 'scheduler.mode'"),
                {"value_json": json.dumps("manual")},
            )
            connection.execute(
                text(
                    "UPDATE extension_rules "
                    "SET enabled = 0, max_size_bytes = 209715200, oversize_action = 'warn' "
                    "WHERE extension = 'aaf'"
                )
            )
            connection.execute(
                text(
                    "UPDATE excluded_patterns "
                    "SET enabled = 0 "
                    "WHERE pattern_type = 'directory_name' AND pattern_value = 'Cache'"
                )
            )

        initialize_database(config)

        with engine.begin() as connection:
            settings_count = connection.execute(text("SELECT COUNT(*) FROM settings")).scalar_one()
            extension_rules_count = connection.execute(
                text("SELECT COUNT(*) FROM extension_rules")
            ).scalar_one()
            excluded_patterns_count = connection.execute(
                text("SELECT COUNT(*) FROM excluded_patterns")
            ).scalar_one()
            scheduler_mode = connection.execute(
                text("SELECT value_json FROM settings WHERE key = 'scheduler.mode'")
            ).scalar_one()
            aaf_rule = connection.execute(
                text(
                    "SELECT enabled, max_size_bytes, oversize_action "
                    "FROM extension_rules WHERE extension = 'aaf'"
                )
            ).one()
            cache_pattern_enabled = connection.execute(
                text(
                    "SELECT enabled FROM excluded_patterns "
                    "WHERE pattern_type = 'directory_name' AND pattern_value = 'Cache'"
                )
            ).scalar_one()
    finally:
        engine.dispose()

    assert settings_count == 6
    assert extension_rules_count == 2
    assert excluded_patterns_count == 3
    assert json.loads(scheduler_mode) == "manual"
    assert aaf_rule == (0, 209715200, "warn")
    assert cache_pattern_enabled == 0


def _load_test_config(tmp_path: Path):
    app_path = tmp_path / "app.yaml"
    rules_path = tmp_path / "rules.yaml"

    app_path.write_text(
        dedent(
            """
            app:
              name: "backup-projects"
              env: "test"
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
              sqlite_path: "runtime/db/test-bootstrap.sqlite3"
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
    rules_path.write_text(
        dedent(
            """
            allowed_extensions:
              - "prproj"
              - "aaf"
            size_limits:
              default_max_size_bytes: null
              by_extension:
                aaf: 104857600
            oversize:
              default_action: "warn"
              aaf_action: "skip"
              log_skipped: true
            exclude_patterns:
              directory_names:
                - "Cache"
              glob_patterns:
                - "**/.cache/**"
              path_substrings:
                - "Autosave"
            unknown_extensions:
              action: "collect_and_skip"
              store_in_registry: true
              log_warning: true
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    return load_config(app_path=app_path, rules_path=rules_path)
