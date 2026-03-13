from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import manual_includes


@dataclass(frozen=True)
class ManualIncludeRecord:
    id: int
    path: str
    include_type: str
    enabled: bool
    created_at: str
    updated_at: str


class ManualIncludesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        *,
        path: str,
        include_type: str,
        created_at: str,
        updated_at: str,
        enabled: bool = True,
    ) -> ManualIncludeRecord:
        result = self._session.execute(
            manual_includes.insert().values(
                path=path,
                include_type=include_type,
                enabled=enabled,
                created_at=created_at,
                updated_at=updated_at,
            )
        )
        manual_include_id = int(result.inserted_primary_key[0])
        record = self.get_by_id(manual_include_id)
        if record is None:
            raise RuntimeError("Failed to load created manual_include record")
        return record

    def get_by_id(self, manual_include_id: int) -> ManualIncludeRecord | None:
        return self._fetch_one(
            select(manual_includes).where(manual_includes.c.id == manual_include_id)
        )

    def get_by_path(self, path: str) -> ManualIncludeRecord | None:
        return self._fetch_one(select(manual_includes).where(manual_includes.c.path == path))

    def list_all(self) -> list[ManualIncludeRecord]:
        return self._fetch_many(select(manual_includes).order_by(manual_includes.c.path))

    def list_enabled(self) -> list[ManualIncludeRecord]:
        return self._fetch_many(
            select(manual_includes)
            .where(manual_includes.c.enabled.is_(True))
            .order_by(manual_includes.c.path)
        )

    def set_enabled(self, manual_include_id: int, *, enabled: bool, updated_at: str) -> None:
        self._session.execute(
            update(manual_includes)
            .where(manual_includes.c.id == manual_include_id)
            .values(enabled=enabled, updated_at=updated_at)
        )

    def _fetch_one(self, statement) -> ManualIncludeRecord | None:
        row = self._session.execute(statement).mappings().one_or_none()
        if row is None:
            return None
        return _to_manual_include_record(row)

    def _fetch_many(self, statement) -> list[ManualIncludeRecord]:
        rows = self._session.execute(statement).mappings().all()
        return [_to_manual_include_record(row) for row in rows]


def _to_manual_include_record(row) -> ManualIncludeRecord:
    return ManualIncludeRecord(
        id=row["id"],
        path=row["path"],
        include_type=row["include_type"],
        enabled=row["enabled"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
