from pathlib import Path

import pytest

from backup_projects.config import (
    ConfigFileNotFoundError,
    ConfigValidationError,
    ConfigYamlParseError,
    load_app_config,
    load_config,
    load_rules_config,
)
from backup_projects.constants import AAF_EXTENSION, AAF_SIZE_LIMIT_BYTES

ROOT = Path(__file__).resolve().parents[1]


def test_load_config_from_example_files_success() -> None:
    config_obj = load_config(
        app_path=ROOT / "config/app.example.yaml",
        rules_path=ROOT / "config/rules.example.yaml",
    )

    assert config_obj.app_config.scheduler.mode == "cron"
    assert config_obj.rules_config.size_limits.by_extension[AAF_EXTENSION] == AAF_SIZE_LIMIT_BYTES
    assert AAF_EXTENSION in config_obj.rules_config.allowed_extensions


def test_load_config_missing_file_error(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-app.yaml"

    with pytest.raises(ConfigFileNotFoundError, match="Config file not found"):
        load_config(
            app_path=missing_path,
            rules_path=ROOT / "config/rules.example.yaml",
        )


def test_load_config_missing_rules_file_error(tmp_path: Path) -> None:
    missing_rules_path = tmp_path / "missing-rules.yaml"

    with pytest.raises(ConfigFileNotFoundError, match="Config file not found"):
        load_config(
            app_path=ROOT / "config/app.example.yaml",
            rules_path=missing_rules_path,
        )


def test_load_app_config_invalid_yaml_error(tmp_path: Path) -> None:
    invalid_yaml_path = tmp_path / "invalid-app.yaml"
    invalid_yaml_path.write_text("app: [broken", encoding="utf-8")

    with pytest.raises(ConfigYamlParseError, match="Invalid YAML"):
        load_app_config(invalid_yaml_path)


def test_load_app_config_schema_validation_error(tmp_path: Path) -> None:
    invalid_schema_path = tmp_path / "invalid-schema-app.yaml"
    invalid_schema_path.write_text(
        """
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
  port: "not-an-int"
  debug: false
db:
  driver: "sqlite"
  sqlite_path: "runtime/db/backup_projects.sqlite3"
restic:
  binary: "restic"
  repository: "/mnt/backup/restic-repo"
  password_env_var: "RESTIC_PASSWORD"
  timeout_seconds: 7200
scheduler:
  mode: "cron"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match="Config schema validation failed"):
        load_app_config(invalid_schema_path)


def test_load_rules_config_schema_validation_error(tmp_path: Path) -> None:
    invalid_rules_path = tmp_path / "invalid-rules.yaml"
    invalid_rules_path.write_text(
        """
allowed_extensions:
  - "aaf"
size_limits:
  default_max_size_bytes: null
  by_extension:
    aaf: 104857600
oversize:
  default_action: "invalid"
  aaf_action: "skip"
  log_skipped: true
exclude_patterns:
  directory_names: []
  glob_patterns: []
  path_substrings: []
unknown_extensions:
  action: "collect_and_skip"
  store_in_registry: true
  log_warning: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match="Config schema validation failed"):
        load_rules_config(invalid_rules_path)
