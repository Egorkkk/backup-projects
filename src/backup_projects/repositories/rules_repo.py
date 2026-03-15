from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import excluded_patterns, extension_rules


@dataclass(frozen=True)
class ExtensionRuleRecord:
    id: int
    extension: str
    enabled: bool
    max_size_bytes: int | None
    oversize_action: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ExcludedPatternRecord:
    id: int
    pattern_type: str
    pattern_value: str
    enabled: bool
    created_at: str
    updated_at: str


class RulesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_extension_rules(self, *, enabled_only: bool = False) -> list[ExtensionRuleRecord]:
        statement = select(extension_rules).order_by(extension_rules.c.extension)
        if enabled_only:
            statement = statement.where(extension_rules.c.enabled.is_(True))
        return self._fetch_extension_rules(statement)

    def get_extension_rule(self, extension: str) -> ExtensionRuleRecord | None:
        row = (
            self._session.execute(
                select(extension_rules).where(extension_rules.c.extension == extension)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return _to_extension_rule_record(row)

    def create_extension_rule(
        self,
        *,
        extension: str,
        enabled: bool,
        max_size_bytes: int | None,
        oversize_action: str,
        created_at: str,
        updated_at: str,
    ) -> ExtensionRuleRecord:
        result = self._session.execute(
            extension_rules.insert().values(
                extension=extension,
                enabled=enabled,
                max_size_bytes=max_size_bytes,
                oversize_action=oversize_action,
                created_at=created_at,
                updated_at=updated_at,
            )
        )
        rule_id = int(result.inserted_primary_key[0])
        record = (
            self._session.execute(select(extension_rules).where(extension_rules.c.id == rule_id))
            .mappings()
            .one()
        )
        return _to_extension_rule_record(record)

    def update_extension_rule(
        self,
        rule_id: int,
        *,
        enabled: bool,
        max_size_bytes: int | None,
        oversize_action: str,
        updated_at: str,
    ) -> None:
        self._session.execute(
            update(extension_rules)
            .where(extension_rules.c.id == rule_id)
            .values(
                enabled=enabled,
                max_size_bytes=max_size_bytes,
                oversize_action=oversize_action,
                updated_at=updated_at,
            )
        )

    def list_excluded_patterns(self, *, enabled_only: bool = False) -> list[ExcludedPatternRecord]:
        statement = select(excluded_patterns).order_by(
            excluded_patterns.c.pattern_type,
            excluded_patterns.c.pattern_value,
        )
        if enabled_only:
            statement = statement.where(excluded_patterns.c.enabled.is_(True))
        rows = self._session.execute(statement).mappings().all()
        return [_to_excluded_pattern_record(row) for row in rows]

    def create_excluded_pattern(
        self,
        *,
        pattern_type: str,
        pattern_value: str,
        enabled: bool,
        created_at: str,
        updated_at: str,
    ) -> ExcludedPatternRecord:
        result = self._session.execute(
            excluded_patterns.insert().values(
                pattern_type=pattern_type,
                pattern_value=pattern_value,
                enabled=enabled,
                created_at=created_at,
                updated_at=updated_at,
            )
        )
        pattern_id = int(result.inserted_primary_key[0])
        row = (
            self._session.execute(
                select(excluded_patterns).where(excluded_patterns.c.id == pattern_id)
            )
            .mappings()
            .one()
        )
        return _to_excluded_pattern_record(row)

    def get_excluded_pattern(self, pattern_id: int) -> ExcludedPatternRecord | None:
        row = (
            self._session.execute(
                select(excluded_patterns).where(excluded_patterns.c.id == pattern_id)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return _to_excluded_pattern_record(row)

    def update_excluded_pattern(
        self,
        pattern_id: int,
        *,
        enabled: bool,
        updated_at: str,
    ) -> None:
        self._session.execute(
            update(excluded_patterns)
            .where(excluded_patterns.c.id == pattern_id)
            .values(enabled=enabled, updated_at=updated_at)
        )

    def _fetch_extension_rules(self, statement) -> list[ExtensionRuleRecord]:
        rows = self._session.execute(statement).mappings().all()
        return [_to_extension_rule_record(row) for row in rows]


def _to_extension_rule_record(row) -> ExtensionRuleRecord:
    return ExtensionRuleRecord(
        id=row["id"],
        extension=row["extension"],
        enabled=row["enabled"],
        max_size_bytes=row["max_size_bytes"],
        oversize_action=row["oversize_action"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _to_excluded_pattern_record(row) -> ExcludedPatternRecord:
    return ExcludedPatternRecord(
        id=row["id"],
        pattern_type=row["pattern_type"],
        pattern_value=row["pattern_value"],
        enabled=row["enabled"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
