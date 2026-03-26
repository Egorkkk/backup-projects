from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.repositories.roots_repo import RootsRepository

_STATUS_FILTER_VALUES = frozenset({"all", "active", "missing"})
_RESCAN_FILTER_VALUES = frozenset({"all", "needs_rescan", "no_rescan"})


@dataclass(frozen=True, slots=True)
class RootsFilterState:
    status: str
    rescan: str


@dataclass(frozen=True, slots=True)
class RootsTableRow:
    id: int
    raid_name: str
    name: str
    path: str
    status: str
    needs_structural_rescan: bool
    last_seen_at: str


@dataclass(frozen=True, slots=True)
class RootsPageView:
    filters: RootsFilterState
    rows: tuple[RootsTableRow, ...]


def build_roots_page_view(
    *,
    session: Session,
    status: str | None,
    rescan: str | None,
) -> RootsPageView:
    filter_state = RootsFilterState(
        status=_normalize_status_filter(status),
        rescan=_normalize_rescan_filter(rescan),
    )

    rows = tuple(
        row
        for row in (_to_table_row(record) for record in RootsRepository(session).list_all())
        if _matches_filters(row, filter_state)
    )

    return RootsPageView(filters=filter_state, rows=rows)


def _normalize_status_filter(value: str | None) -> str:
    if value in _STATUS_FILTER_VALUES:
        return value
    return "all"


def _normalize_rescan_filter(value: str | None) -> str:
    if value in _RESCAN_FILTER_VALUES:
        return value
    return "all"


def _to_table_row(record) -> RootsTableRow:
    return RootsTableRow(
        id=record.id,
        raid_name=record.raid_name,
        name=record.name,
        path=record.path,
        status="missing" if record.is_missing else "active",
        needs_structural_rescan=record.needs_structural_rescan,
        last_seen_at=record.last_seen_at,
    )


def _matches_filters(row: RootsTableRow, filters: RootsFilterState) -> bool:
    if filters.status != "all" and row.status != filters.status:
        return False
    if filters.rescan == "needs_rescan" and not row.needs_structural_rescan:
        return False
    if filters.rescan == "no_rescan" and row.needs_structural_rescan:
        return False
    return True
