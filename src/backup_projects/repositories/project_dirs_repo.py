from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import project_dirs


@dataclass(frozen=True)
class ProjectDirRecord:
    id: int
    root_id: int
    relative_path: str
    name: str
    dir_type: str
    is_missing: bool
    first_seen_at: str
    last_seen_at: str


class ProjectDirsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        *,
        root_id: int,
        relative_path: str,
        name: str,
        dir_type: str,
        first_seen_at: str,
        last_seen_at: str,
        is_missing: bool = False,
    ) -> ProjectDirRecord:
        result = self._session.execute(
            project_dirs.insert().values(
                root_id=root_id,
                relative_path=relative_path,
                name=name,
                dir_type=dir_type,
                is_missing=is_missing,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
            )
        )
        project_dir_id = int(result.inserted_primary_key[0])
        record = self.get_by_id(project_dir_id)
        if record is None:
            raise RuntimeError("Failed to load created project_dir record")
        return record

    def get_by_id(self, project_dir_id: int) -> ProjectDirRecord | None:
        return self._fetch_one(select(project_dirs).where(project_dirs.c.id == project_dir_id))

    def get_by_root_and_path(self, *, root_id: int, relative_path: str) -> ProjectDirRecord | None:
        return self._fetch_one(
            select(project_dirs).where(
                project_dirs.c.root_id == root_id,
                project_dirs.c.relative_path == relative_path,
            )
        )

    def list_by_root(self, root_id: int) -> list[ProjectDirRecord]:
        return self._fetch_many(
            select(project_dirs)
            .where(project_dirs.c.root_id == root_id)
            .order_by(project_dirs.c.relative_path)
        )

    def list_active_by_root(self, root_id: int) -> list[ProjectDirRecord]:
        return self._fetch_many(
            select(project_dirs)
            .where(project_dirs.c.root_id == root_id, project_dirs.c.is_missing.is_(False))
            .order_by(project_dirs.c.relative_path)
        )

    def update_scan_state(
        self,
        project_dir_id: int,
        *,
        dir_type: str,
        is_missing: bool,
        last_seen_at: str,
    ) -> None:
        self._session.execute(
            update(project_dirs)
            .where(project_dirs.c.id == project_dir_id)
            .values(dir_type=dir_type, is_missing=is_missing, last_seen_at=last_seen_at)
        )

    def _fetch_one(self, statement) -> ProjectDirRecord | None:
        row = self._session.execute(statement).mappings().one_or_none()
        if row is None:
            return None
        return _to_project_dir_record(row)

    def _fetch_many(self, statement) -> list[ProjectDirRecord]:
        rows = self._session.execute(statement).mappings().all()
        return [_to_project_dir_record(row) for row in rows]


def _to_project_dir_record(row) -> ProjectDirRecord:
    return ProjectDirRecord(
        id=row["id"],
        root_id=row["root_id"],
        relative_path=row["relative_path"],
        name=row["name"],
        dir_type=row["dir_type"],
        is_missing=row["is_missing"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
    )
