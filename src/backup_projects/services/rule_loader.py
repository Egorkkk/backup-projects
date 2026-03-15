from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.converters import (
    to_domain_oversize_action,
    to_excluded_pattern,
    to_extension_rule,
)
from backup_projects.domain.enums import OversizeAction
from backup_projects.domain.models import ExcludedPattern, ExtensionRule
from backup_projects.repositories.rules_repo import RulesRepository
from backup_projects.repositories.settings_repo import SettingRecord, SettingsRepository


@dataclass(frozen=True, slots=True)
class LoadedPolicySettings:
    oversize_default_action: OversizeAction
    oversize_log_skipped: bool
    unknown_extensions_action: str
    unknown_extensions_store_in_registry: bool
    unknown_extensions_log_warning: bool


@dataclass(frozen=True, slots=True)
class LoadedPolicyConfig:
    extension_rules: tuple[ExtensionRule, ...]
    excluded_patterns: tuple[ExcludedPattern, ...]
    settings: LoadedPolicySettings


def load_policy_config(*, session: Session) -> LoadedPolicyConfig:
    rules_repo = RulesRepository(session)
    settings_repo = SettingsRepository(session)

    extension_rules = tuple(
        to_extension_rule(record)
        for record in rules_repo.list_extension_rules(enabled_only=True)
    )
    excluded_patterns = tuple(
        to_excluded_pattern(record)
        for record in rules_repo.list_excluded_patterns(enabled_only=True)
    )
    settings_by_key = {
        record.key: record
        for record in settings_repo.list_settings()
    }

    return LoadedPolicyConfig(
        extension_rules=extension_rules,
        excluded_patterns=excluded_patterns,
        settings=_load_policy_settings(settings_by_key=settings_by_key),
    )


def _load_policy_settings(
    *,
    settings_by_key: dict[str, SettingRecord],
) -> LoadedPolicySettings:
    oversize_default_action = to_domain_oversize_action(
        _load_required_setting_value(
            settings_by_key=settings_by_key,
            key="oversize.default_action",
            expected_type=str,
        )
    )
    oversize_log_skipped = _load_required_setting_value(
        settings_by_key=settings_by_key,
        key="oversize.log_skipped",
        expected_type=bool,
    )
    unknown_extensions_action = _load_required_setting_value(
        settings_by_key=settings_by_key,
        key="unknown_extensions.action",
        expected_type=str,
    )
    unknown_extensions_store_in_registry = _load_required_setting_value(
        settings_by_key=settings_by_key,
        key="unknown_extensions.store_in_registry",
        expected_type=bool,
    )
    unknown_extensions_log_warning = _load_required_setting_value(
        settings_by_key=settings_by_key,
        key="unknown_extensions.log_warning",
        expected_type=bool,
    )

    return LoadedPolicySettings(
        oversize_default_action=oversize_default_action,
        oversize_log_skipped=oversize_log_skipped,
        unknown_extensions_action=unknown_extensions_action,
        unknown_extensions_store_in_registry=unknown_extensions_store_in_registry,
        unknown_extensions_log_warning=unknown_extensions_log_warning,
    )


def _load_required_setting_value(
    *,
    settings_by_key: dict[str, SettingRecord],
    key: str,
    expected_type: type[str] | type[bool],
) -> str | bool:
    record = settings_by_key.get(key)
    if record is None:
        raise ValueError(f"Required setting is missing: {key}")

    try:
        parsed_value = json.loads(record.value_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON for setting '{key}': {exc}") from exc

    if not isinstance(parsed_value, expected_type):
        expected_name = expected_type.__name__
        actual_name = type(parsed_value).__name__
        raise ValueError(
            f"Invalid value type for setting '{key}': expected {expected_name}, got {actual_name}"
        )

    return parsed_value
