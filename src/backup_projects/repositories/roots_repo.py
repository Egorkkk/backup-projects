from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import roots


@dataclass(frozen=True)
class RootRecord:
    id: int
    raid_name: str
    name: str
    path: str
    device_id: int | None
    inode: int | None
    mtime_ns: int | None
    ctime_ns: int | None
    is_missing: bool
    needs_structural_rescan: bool
    first_seen_at: str
    last_seen_at: str


class RootsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        *,
        raid_name: str,
        name: str,
        path: str,
        device_id: int | None,
        inode: int | None,
        mtime_ns: int | None,
        ctime_ns: int | None,
        first_seen_at: str,
        last_seen_at: str,
        is_missing: bool = False,
        needs_structural_rescan: bool = False,
    ) -> RootRecord:
        result = self._session.execute(
            roots.insert().values(
                raid_name=raid_name,
                name=name,
                path=path,
                device_id=device_id,
                inode=inode,
                mtime_ns=mtime_ns,
                ctime_ns=ctime_ns,
                is_missing=is_missing,
                needs_structural_rescan=needs_structural_rescan,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
            )
        )
        root_id = int(result.inserted_primary_key[0])
        record = self.get_by_id(root_id)
        if record is None:
            raise RuntimeError("Failed to load created root record")
        return record

    def get_by_id(self, root_id: int) -> RootRecord | None:
        return self._fetch_one(select(roots).where(roots.c.id == root_id))

    def get_by_path(self, path: str) -> RootRecord | None:
        return self._fetch_one(select(roots).where(roots.c.path == path))

    def list_all(self) -> list[RootRecord]:
        return self._fetch_many(select(roots).order_by(roots.c.path))

    def list_active(self) -> list[RootRecord]:
        return self._fetch_many(
            select(roots).where(roots.c.is_missing.is_(False)).order_by(roots.c.path)
        )

    def mark_missing(self, root_id: int, *, last_seen_at: str) -> None:
        self._session.execute(
            update(roots)
            .where(roots.c.id == root_id)
            .values(is_missing=True, last_seen_at=last_seen_at)
        )

    def mark_present(
        self,
        root_id: int,
        *,
        device_id: int | None,
        inode: int | None,
        mtime_ns: int | None,
        ctime_ns: int | None,
        last_seen_at: str,
        needs_structural_rescan: bool,
    ) -> None:
        self._session.execute(
            update(roots)
            .where(roots.c.id == root_id)
            .values(
                device_id=device_id,
                inode=inode,
                mtime_ns=mtime_ns,
                ctime_ns=ctime_ns,
                is_missing=False,
                last_seen_at=last_seen_at,
                needs_structural_rescan=needs_structural_rescan,
            )
        )

    def _fetch_one(self, statement) -> RootRecord | None:
        row = self._session.execute(statement).mappings().one_or_none()
        if row is None:
            return None
        return _to_root_record(row)

    def _fetch_many(self, statement) -> list[RootRecord]:
        rows = self._session.execute(statement).mappings().all()
        return [_to_root_record(row) for row in rows]


def _to_root_record(row) -> RootRecord:
    return RootRecord(
        id=row["id"],
        raid_name=row["raid_name"],
        name=row["name"],
        path=row["path"],
        device_id=row["device_id"],
        inode=row["inode"],
        mtime_ns=row["mtime_ns"],
        ctime_ns=row["ctime_ns"],
        is_missing=row["is_missing"],
        needs_structural_rescan=row["needs_structural_rescan"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
    )
