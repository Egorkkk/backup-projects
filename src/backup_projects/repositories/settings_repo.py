from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import settings


@dataclass(frozen=True)
class SettingRecord:
    id: int
    key: str
    value_json: str
    updated_at: str


class SettingsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_settings(self) -> list[SettingRecord]:
        rows = self._session.execute(select(settings).order_by(settings.c.key)).mappings().all()
        return [_to_setting_record(row) for row in rows]

    def get_setting(self, key: str) -> SettingRecord | None:
        row = (
            self._session.execute(select(settings).where(settings.c.key == key))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return _to_setting_record(row)

    def set_setting(self, *, key: str, value_json: str, updated_at: str) -> None:
        statement = sqlite_insert(settings).values(
            key=key,
            value_json=value_json,
            updated_at=updated_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=["key"],
            set_={"value_json": value_json, "updated_at": updated_at},
        )
        self._session.execute(statement)


def _to_setting_record(row) -> SettingRecord:
    return SettingRecord(
        id=row["id"],
        key=row["key"],
        value_json=row["value_json"],
        updated_at=row["updated_at"],
    )
