from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backup_projects.constants import AAF_SIZE_LIMIT_BYTES, OversizeAction
from backup_projects.repositories.rules_repo import RulesRepository

_SUPPORTED_EXCLUDE_PATTERN_TYPES = (
    "directory_name",
    "glob",
    "path_substring",
    "regex",
)


@dataclass(frozen=True, slots=True)
class ExtensionRuleRow:
    id: int
    extension: str
    enabled: bool
    max_size_bytes: int | None
    oversize_action: str


@dataclass(frozen=True, slots=True)
class ExcludedPatternRow:
    id: int
    pattern_type: str
    pattern_value: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class RulesPageView:
    extension_rules: tuple[ExtensionRuleRow, ...]
    excluded_patterns: tuple[ExcludedPatternRow, ...]
    aaf_default_size_bytes: int


def build_rules_page_view(*, session: Session) -> RulesPageView:
    repo = RulesRepository(session)
    return RulesPageView(
        extension_rules=tuple(
            ExtensionRuleRow(
                id=rule.id,
                extension=rule.extension,
                enabled=rule.enabled,
                max_size_bytes=rule.max_size_bytes,
                oversize_action=rule.oversize_action,
            )
            for rule in repo.list_extension_rules()
        ),
        excluded_patterns=tuple(
            ExcludedPatternRow(
                id=pattern.id,
                pattern_type=pattern.pattern_type,
                pattern_value=pattern.pattern_value,
                enabled=pattern.enabled,
            )
            for pattern in repo.list_excluded_patterns()
        ),
        aaf_default_size_bytes=AAF_SIZE_LIMIT_BYTES,
    )


def create_extension_rule(
    *,
    session: Session,
    extension: str,
    enabled: bool,
    max_size_bytes_raw: str | None,
    oversize_action: str,
) -> None:
    repo = RulesRepository(session)
    normalized_extension = _normalize_extension(extension)
    normalized_max_size = _normalize_max_size_bytes(max_size_bytes_raw)
    normalized_oversize_action = _normalize_oversize_action(oversize_action)
    now_iso = _now_iso()

    try:
        repo.create_extension_rule(
            extension=normalized_extension,
            enabled=enabled,
            max_size_bytes=normalized_max_size,
            oversize_action=normalized_oversize_action,
            created_at=now_iso,
            updated_at=now_iso,
        )
    except IntegrityError as exc:
        raise ValueError(
            f"Extension rule already exists for extension: {normalized_extension}"
        ) from exc


def update_extension_rule(
    *,
    session: Session,
    extension: str,
    enabled: bool,
    max_size_bytes_raw: str | None,
    clear_max_size: bool,
    oversize_action: str,
) -> None:
    repo = RulesRepository(session)
    normalized_extension = _normalize_extension(extension)
    existing_rule = repo.get_extension_rule(normalized_extension)
    if existing_rule is None:
        raise LookupError(f"Extension rule not found for extension: {normalized_extension}")

    normalized_oversize_action = _normalize_oversize_action(oversize_action)
    if clear_max_size:
        normalized_max_size = None
    else:
        normalized_max_size = _normalize_max_size_bytes(max_size_bytes_raw)

    repo.update_extension_rule(
        existing_rule.id,
        enabled=enabled,
        max_size_bytes=normalized_max_size,
        oversize_action=normalized_oversize_action,
        updated_at=_now_iso(),
    )


def create_excluded_pattern(
    *,
    session: Session,
    pattern_type: str,
    pattern_value: str,
    enabled: bool,
) -> None:
    repo = RulesRepository(session)
    normalized_pattern_type = _normalize_pattern_type(pattern_type)
    normalized_pattern_value = _normalize_pattern_value(pattern_value)
    now_iso = _now_iso()

    try:
        repo.create_excluded_pattern(
            pattern_type=normalized_pattern_type,
            pattern_value=normalized_pattern_value,
            enabled=enabled,
            created_at=now_iso,
            updated_at=now_iso,
        )
    except IntegrityError as exc:
        raise ValueError(
            "Excluded pattern already exists: "
            f"{normalized_pattern_type}:{normalized_pattern_value}"
        ) from exc


def toggle_excluded_pattern(
    *,
    session: Session,
    pattern_id: int,
) -> None:
    repo = RulesRepository(session)
    existing_pattern = repo.get_excluded_pattern(pattern_id)
    if existing_pattern is None:
        raise LookupError(f"Excluded pattern not found for id: {pattern_id}")

    repo.update_excluded_pattern(
        pattern_id,
        enabled=not existing_pattern.enabled,
        updated_at=_now_iso(),
    )


def _normalize_extension(extension: str) -> str:
    normalized_extension = extension.strip().lower()
    if normalized_extension.startswith("."):
        normalized_extension = normalized_extension[1:]
    if normalized_extension == "":
        raise ValueError("extension must not be empty")
    return normalized_extension


def _normalize_max_size_bytes(value: str | None) -> int | None:
    if value is None:
        return None

    normalized_value = value.strip()
    if normalized_value == "":
        return None

    try:
        parsed_value = int(normalized_value)
    except ValueError as exc:
        raise ValueError("max_size_bytes must be an integer") from exc

    if parsed_value < 0:
        raise ValueError("max_size_bytes must be >= 0")

    return parsed_value


def _normalize_oversize_action(value: str) -> str:
    normalized_value = value.strip().lower()
    allowed_values = {action.value for action in OversizeAction}
    if normalized_value not in allowed_values:
        raise ValueError(
            "oversize_action must be one of: "
            + ", ".join(sorted(allowed_values))
        )
    return normalized_value


def _normalize_pattern_type(value: str) -> str:
    normalized_value = value.strip()
    if normalized_value not in _SUPPORTED_EXCLUDE_PATTERN_TYPES:
        raise ValueError(
            "pattern_type must be one of: "
            + ", ".join(_SUPPORTED_EXCLUDE_PATTERN_TYPES)
        )
    return normalized_value


def _normalize_pattern_value(value: str) -> str:
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError("pattern_value must not be empty")
    return normalized_value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
