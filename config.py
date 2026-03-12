from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class ConfigError(Exception):
    """Base exception for config loading errors."""


class ConfigFileNotFoundError(ConfigError):
    """Raised when a config file is missing."""


class ConfigYamlParseError(ConfigError):
    """Raised when a config file contains invalid YAML syntax."""


class ConfigValidationError(ConfigError):
    """Raised when config data does not match expected schema."""


class StrictConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AppSettings(StrictConfigModel):
    name: str
    env: str
    log_level: str


class RaidRoot(StrictConfigModel):
    name: str
    path: str
    enabled: bool


class RuntimePaths(StrictConfigModel):
    logs_dir: str
    manifests_dir: str
    reports_dir: str
    db_dir: str
    locks_dir: str


class WebSettings(StrictConfigModel):
    host: str
    port: int
    debug: bool


class DbSettings(StrictConfigModel):
    driver: Literal["sqlite"]
    sqlite_path: str


class ResticSettings(StrictConfigModel):
    binary: str
    repository: str
    password_env_var: str
    timeout_seconds: int

    @field_validator("timeout_seconds")
    @classmethod
    def validate_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("timeout_seconds must be > 0")
        return value


class SchedulerSettings(StrictConfigModel):
    mode: Literal["cron"]


class AppFileConfig(StrictConfigModel):
    app: AppSettings
    raid_roots: list[RaidRoot]
    runtime: RuntimePaths
    web: WebSettings
    db: DbSettings
    restic: ResticSettings
    scheduler: SchedulerSettings


class SizeLimits(StrictConfigModel):
    default_max_size_bytes: int | None = None
    by_extension: dict[str, int]

    @field_validator("default_max_size_bytes")
    @classmethod
    def validate_default_max_size_bytes(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("default_max_size_bytes must be > 0 or null")
        return value

    @field_validator("by_extension")
    @classmethod
    def validate_by_extension(cls, value: dict[str, int]) -> dict[str, int]:
        if "aaf" not in value:
            raise ValueError("size_limits.by_extension must define 'aaf'")
        for extension, limit in value.items():
            if not extension:
                raise ValueError("extension key must be non-empty")
            if limit <= 0:
                raise ValueError(f"size limit for extension '{extension}' must be > 0")
        return value


class ExcludePatterns(StrictConfigModel):
    directory_names: list[str]
    glob_patterns: list[str]
    path_substrings: list[str]


class OversizePolicy(StrictConfigModel):
    default_action: Literal["skip", "warn", "include"]
    aaf_action: Literal["skip", "warn", "include"]
    log_skipped: bool


class UnknownExtensionsPolicy(StrictConfigModel):
    action: Literal["collect_and_skip", "skip_silent"]
    store_in_registry: bool
    log_warning: bool


class RulesConfig(StrictConfigModel):
    allowed_extensions: list[str]
    size_limits: SizeLimits
    oversize: OversizePolicy
    exclude_patterns: ExcludePatterns
    unknown_extensions: UnknownExtensionsPolicy

    @field_validator("allowed_extensions")
    @classmethod
    def validate_allowed_extensions(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("allowed_extensions must contain at least one extension")

        normalized: list[str] = []
        seen: set[str] = set()
        for raw in value:
            extension = raw.strip().lower()
            if extension.startswith("."):
                extension = extension[1:]
            if not extension:
                raise ValueError("allowed_extensions cannot contain empty values")
            if extension not in seen:
                seen.add(extension)
                normalized.append(extension)
        return normalized


class ProjectConfig(StrictConfigModel):
    app_config: AppFileConfig
    rules_config: RulesConfig
    app_path: Path
    rules_path: Path


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigFileNotFoundError(f"Config file not found: {path}") from exc

    try:
        parsed = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise ConfigYamlParseError(f"Invalid YAML in '{path}': {exc}") from exc

    if parsed is None:
        raise ConfigValidationError(f"Config file '{path}' is empty; expected a YAML mapping")
    if not isinstance(parsed, dict):
        raise ConfigValidationError(
            f"Invalid config root in '{path}': expected a YAML mapping/object"
        )

    return parsed


def _validate_model(model_cls: type[StrictConfigModel], data: dict[str, Any], path: Path) -> Any:
    try:
        return model_cls.model_validate(data)
    except ValidationError as exc:
        raise ConfigValidationError(f"Config schema validation failed for '{path}': {exc}") from exc


def load_app_config(app_path: str | Path) -> AppFileConfig:
    path = Path(app_path)
    data = _read_yaml_mapping(path)
    return _validate_model(AppFileConfig, data, path)


def load_rules_config(rules_path: str | Path) -> RulesConfig:
    path = Path(rules_path)
    data = _read_yaml_mapping(path)
    return _validate_model(RulesConfig, data, path)


def load_config(
    app_path: str | Path = "config/app.yaml",
    rules_path: str | Path = "config/rules.yaml",
) -> ProjectConfig:
    app_path_obj = Path(app_path)
    rules_path_obj = Path(rules_path)

    app_config = load_app_config(app_path_obj)
    rules_config = load_rules_config(rules_path_obj)

    return ProjectConfig(
        app_config=app_config,
        rules_config=rules_config,
        app_path=app_path_obj,
        rules_path=rules_path_obj,
    )
