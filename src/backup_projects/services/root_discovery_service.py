from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.dir_listing import list_dir
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.adapters.filesystem.stat_reader import read_stat
from backup_projects.converters import to_root_record
from backup_projects.domain.models import RootRecord
from backup_projects.repositories.roots_repo import RootRecord as RepoRootRecord
from backup_projects.repositories.roots_repo import RootsRepository


@dataclass(frozen=True, slots=True)
class DiscoveredRootCandidate:
    name: str
    path: str


@dataclass(frozen=True, slots=True)
class RootDiscoveryResult:
    discovered: tuple[RootRecord, ...]
    created: tuple[RootRecord, ...]
    marked_missing: tuple[RootRecord, ...]
    reactivated: tuple[RootRecord, ...]
    unchanged_present: tuple[RootRecord, ...]


def list_root_directories(raid_path: str | Path) -> tuple[DiscoveredRootCandidate, ...]:
    resolved_raid_path = resolve_path(raid_path)
    return tuple(
        DiscoveredRootCandidate(
            name=entry.name,
            path=entry.path.resolve(strict=False).as_posix(),
        )
        for entry in list_dir(resolved_raid_path)
        if entry.is_dir
    )


def discover_and_sync_roots(
    *,
    session: Session,
    raid_name: str,
    raid_path: str | Path,
    discovered_at: str,
) -> RootDiscoveryResult:
    repo = RootsRepository(session)
    existing_roots = [
        record for record in repo.list_all() if record.raid_name == raid_name
    ]
    existing_by_path = {record.path: record for record in existing_roots}
    discovered_candidates = list_root_directories(raid_path)

    discovered_paths: set[str] = set()
    discovered_records: list[RootRecord] = []
    created_records: list[RootRecord] = []
    marked_missing_records: list[RootRecord] = []
    reactivated_records: list[RootRecord] = []
    unchanged_present_records: list[RootRecord] = []

    for candidate in discovered_candidates:
        discovered_paths.add(candidate.path)
        stat_info = read_stat(candidate.path)
        if stat_info is None:
            continue
        if not stat_info.is_dir:
            raise NotADirectoryError(candidate.path)

        existing_record = existing_by_path.get(candidate.path)
        if existing_record is None:
            created_record = repo.create(
                raid_name=raid_name,
                name=candidate.name,
                path=candidate.path,
                device_id=stat_info.device_id,
                inode=stat_info.inode,
                mtime_ns=stat_info.mtime_ns,
                ctime_ns=stat_info.ctime_ns,
                first_seen_at=discovered_at,
                last_seen_at=discovered_at,
                is_missing=False,
                needs_structural_rescan=True,
            )
            domain_record = to_root_record(created_record)
            created_records.append(domain_record)
            discovered_records.append(domain_record)
            continue

        needs_structural_rescan = _compute_needs_structural_rescan(existing_record, stat_info)
        repo.mark_present(
            existing_record.id,
            device_id=stat_info.device_id,
            inode=stat_info.inode,
            mtime_ns=stat_info.mtime_ns,
            ctime_ns=stat_info.ctime_ns,
            last_seen_at=discovered_at,
            needs_structural_rescan=needs_structural_rescan,
        )
        refreshed_record = _reload_root_by_id(repo, existing_record.id)
        domain_record = to_root_record(refreshed_record)
        discovered_records.append(domain_record)
        if existing_record.is_missing:
            reactivated_records.append(domain_record)
        else:
            unchanged_present_records.append(domain_record)

    for existing_record in existing_roots:
        if existing_record.path in discovered_paths:
            continue

        repo.mark_missing(existing_record.id, last_seen_at=discovered_at)
        refreshed_record = _reload_root_by_id(repo, existing_record.id)
        marked_missing_records.append(to_root_record(refreshed_record))

    return RootDiscoveryResult(
        discovered=tuple(discovered_records),
        created=tuple(created_records),
        marked_missing=tuple(marked_missing_records),
        reactivated=tuple(reactivated_records),
        unchanged_present=tuple(unchanged_present_records),
    )


def _reload_root_by_id(repo: RootsRepository, root_id: int) -> RepoRootRecord:
    record = repo.get_by_id(root_id)
    if record is None:
        raise RuntimeError(f"Failed to reload root record {root_id}")
    return record


def _compute_needs_structural_rescan(
    existing_record: RepoRootRecord,
    stat_info,
) -> bool:
    if existing_record.is_missing:
        return True
    if _did_root_identity_change(existing_record, stat_info):
        return True
    return existing_record.needs_structural_rescan


def _did_root_identity_change(existing_record: RepoRootRecord, stat_info) -> bool:
    return (
        existing_record.inode != stat_info.inode
        or existing_record.mtime_ns != stat_info.mtime_ns
        or existing_record.ctime_ns != stat_info.ctime_ns
    )
