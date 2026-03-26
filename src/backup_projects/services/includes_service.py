from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.manual_include_crud_service import (
    create_manual_directory_include,
    create_manual_file_include,
)


@dataclass(frozen=True, slots=True)
class IncludeRootOption:
    id: int
    name: str


@dataclass(frozen=True, slots=True)
class IncludeRow:
    id: int
    root_id: int
    root_name: str
    relative_path: str
    include_path_type: str
    recursive: bool
    force_include: bool
    enabled: bool
    updated_at: str


@dataclass(frozen=True, slots=True)
class IncludesPageView:
    available_roots: tuple[IncludeRootOption, ...]
    rows: tuple[IncludeRow, ...]


def build_includes_page_view(*, session: Session) -> IncludesPageView:
    roots_repo = RootsRepository(session)
    includes_repo = ManualIncludesRepository(session)
    roots = tuple(roots_repo.list_all())
    roots_by_id = {root.id: root for root in roots}

    rows: list[IncludeRow] = []
    for root in roots:
        for manual_include in includes_repo.list_by_root(root.id):
            rows.append(
                IncludeRow(
                    id=manual_include.id,
                    root_id=manual_include.root_id,
                    root_name=root.name,
                    relative_path=manual_include.relative_path,
                    include_path_type=manual_include.include_path_type,
                    recursive=manual_include.recursive,
                    force_include=manual_include.force_include,
                    enabled=manual_include.enabled,
                    updated_at=manual_include.updated_at,
                )
            )

    return IncludesPageView(
        available_roots=tuple(
            IncludeRootOption(id=root.id, name=root.name)
            for root in roots
            if not root.is_missing
        ),
        rows=tuple(
            sorted(
                rows,
                key=lambda row: (
                    roots_by_id.get(row.root_id).path if row.root_id in roots_by_id else "",
                    row.relative_path,
                    row.id,
                ),
            )
        ),
    )


def create_include(
    *,
    session: Session,
    root_id_raw: str,
    target_path: str,
    include_path_type: str,
    recursive: bool,
    force_include: bool,
    enabled: bool,
) -> None:
    root_id = _normalize_root_id(root_id_raw)
    normalized_include_path_type = _normalize_include_path_type(include_path_type)
    now_iso = _now_iso()

    if normalized_include_path_type == "file":
        create_manual_file_include(
            session=session,
            root_id=root_id,
            target_path=target_path,
            created_at=now_iso,
            force_include=force_include,
            enabled=enabled,
        )
        return

    create_manual_directory_include(
        session=session,
        root_id=root_id,
        target_path=target_path,
        created_at=now_iso,
        recursive=recursive,
        force_include=force_include,
        enabled=enabled,
    )


def toggle_include_enabled(
    *,
    session: Session,
    include_id: int,
) -> None:
    repo = ManualIncludesRepository(session)
    existing_include = repo.get_by_id(include_id)
    if existing_include is None:
        raise LookupError(f"Manual include not found for id: {include_id}")

    repo.update(
        include_id,
        relative_path=existing_include.relative_path,
        include_path_type=existing_include.include_path_type,
        recursive=existing_include.recursive,
        force_include=existing_include.force_include,
        enabled=not existing_include.enabled,
        updated_at=_now_iso(),
    )


def delete_include(
    *,
    session: Session,
    include_id: int,
) -> None:
    repo = ManualIncludesRepository(session)
    existing_include = repo.get_by_id(include_id)
    if existing_include is None:
        raise LookupError(f"Manual include not found for id: {include_id}")
    repo.delete(include_id)


def _normalize_root_id(value: str) -> int:
    try:
        root_id = int(value)
    except ValueError as exc:
        raise ValueError("root_id must be an integer") from exc
    if root_id <= 0:
        raise ValueError("root_id must be > 0")
    return root_id


def _normalize_include_path_type(value: str) -> str:
    normalized_value = value.strip().lower()
    if normalized_value not in {"file", "directory"}:
        raise ValueError("include_path_type must be one of: directory, file")
    return normalized_value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
