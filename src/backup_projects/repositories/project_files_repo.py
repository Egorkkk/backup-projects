from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import project_files


@dataclass(frozen=True)
class ProjectFileRecord:
    id: int
    project_dir_id: int
    relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None
    is_missing: bool
    first_seen_at: str
    last_seen_at: str


class ProjectFilesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(
        self,
        *,
        project_dir_id: int,
        relative_path: str,
        filename: str,
        extension: str,
        size_bytes: int,
        mtime_ns: int,
        ctime_ns: int,
        inode: int | None,
        first_seen_at: str,
        last_seen_at: str,
        is_missing: bool = False,
    ) -> ProjectFileRecord:
        result = self._session.execute(
            project_files.insert().values(
                project_dir_id=project_dir_id,
                relative_path=relative_path,
                filename=filename,
                extension=extension,
                size_bytes=size_bytes,
                mtime_ns=mtime_ns,
                ctime_ns=ctime_ns,
                inode=inode,
                is_missing=is_missing,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
            )
        )
        project_file_id = int(result.inserted_primary_key[0])
        record = self.get_by_id(project_file_id)
        if record is None:
            raise RuntimeError("Failed to load created project_file record")
        return record

    def get_by_id(self, project_file_id: int) -> ProjectFileRecord | None:
        return self._fetch_one(select(project_files).where(project_files.c.id == project_file_id))

    def get_by_dir_and_path(
        self, *, project_dir_id: int, relative_path: str
    ) -> ProjectFileRecord | None:
        return self._fetch_one(
            select(project_files).where(
                project_files.c.project_dir_id == project_dir_id,
                project_files.c.relative_path == relative_path,
            )
        )

    def list_by_project_dir(self, project_dir_id: int) -> list[ProjectFileRecord]:
        return self._fetch_many(
            select(project_files)
            .where(project_files.c.project_dir_id == project_dir_id)
            .order_by(project_files.c.relative_path)
        )

    def update_stat_fields(
        self,
        project_file_id: int,
        *,
        size_bytes: int,
        mtime_ns: int,
        ctime_ns: int,
        inode: int | None,
        is_missing: bool,
        last_seen_at: str,
    ) -> None:
        self._session.execute(
            update(project_files)
            .where(project_files.c.id == project_file_id)
            .values(
                size_bytes=size_bytes,
                mtime_ns=mtime_ns,
                ctime_ns=ctime_ns,
                inode=inode,
                is_missing=is_missing,
                last_seen_at=last_seen_at,
            )
        )

    def mark_missing(self, project_file_id: int, *, last_seen_at: str) -> None:
        self._session.execute(
            update(project_files)
            .where(project_files.c.id == project_file_id)
            .values(is_missing=True, last_seen_at=last_seen_at)
        )

    def _fetch_one(self, statement) -> ProjectFileRecord | None:
        row = self._session.execute(statement).mappings().one_or_none()
        if row is None:
            return None
        return _to_project_file_record(row)

    def _fetch_many(self, statement) -> list[ProjectFileRecord]:
        rows = self._session.execute(statement).mappings().all()
        return [_to_project_file_record(row) for row in rows]


def _to_project_file_record(row) -> ProjectFileRecord:
    return ProjectFileRecord(
        id=row["id"],
        project_dir_id=row["project_dir_id"],
        relative_path=row["relative_path"],
        filename=row["filename"],
        extension=row["extension"],
        size_bytes=row["size_bytes"],
        mtime_ns=row["mtime_ns"],
        ctime_ns=row["ctime_ns"],
        inode=row["inode"],
        is_missing=row["is_missing"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
    )
