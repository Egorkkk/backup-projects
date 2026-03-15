from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import manual_includes


@dataclass(frozen=True)
class ManualIncludeRecord:
    id: int
    root_id: int
    relative_path: str
    include_path_type: str
    recursive: bool
    force_include: bool
    enabled: bool
    created_at: str
    updated_at: str


class ManualIncludesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        *,
        root_id: int,
        relative_path: str,
        include_path_type: str,
        created_at: str,
        updated_at: str,
        recursive: bool = False,
        force_include: bool = False,
        enabled: bool = True,
    ) -> ManualIncludeRecord:
        result = self._session.execute(
            manual_includes.insert().values(
                root_id=root_id,
                relative_path=relative_path,
                include_path_type=include_path_type,
                recursive=recursive,
                force_include=force_include,
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

    def get_by_root_and_path(
        self,
        *,
        root_id: int,
        relative_path: str,
    ) -> ManualIncludeRecord | None:
        return self._fetch_one(
            select(manual_includes).where(
                manual_includes.c.root_id == root_id,
                manual_includes.c.relative_path == relative_path,
            )
        )

    def list_by_root(self, root_id: int) -> list[ManualIncludeRecord]:
        return self._fetch_many(
            select(manual_includes)
            .where(manual_includes.c.root_id == root_id)
            .order_by(manual_includes.c.relative_path)
        )

    def list_enabled_by_root(self, root_id: int) -> list[ManualIncludeRecord]:
        return self._fetch_many(
            select(manual_includes)
            .where(
                manual_includes.c.root_id == root_id,
                manual_includes.c.enabled.is_(True),
            )
            .order_by(manual_includes.c.relative_path)
        )

    def update(
        self,
        manual_include_id: int,
        *,
        relative_path: str,
        include_path_type: str,
        recursive: bool,
        force_include: bool,
        enabled: bool,
        updated_at: str,
    ) -> None:
        self._session.execute(
            update(manual_includes)
            .where(manual_includes.c.id == manual_include_id)
            .values(
                relative_path=relative_path,
                include_path_type=include_path_type,
                recursive=recursive,
                force_include=force_include,
                enabled=enabled,
                updated_at=updated_at,
            )
        )

    def delete(self, manual_include_id: int) -> None:
        self._session.execute(
            delete(manual_includes).where(manual_includes.c.id == manual_include_id)
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
        root_id=row["root_id"],
        relative_path=row["relative_path"],
        include_path_type=row["include_path_type"],
        recursive=row["recursive"],
        force_include=row["force_include"],
        enabled=row["enabled"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
