from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.repositories.roots_repo import RootRecord, RootsRepository
from backup_projects.services.dry_run_service import build_root_dry_run_manifest


@dataclass(frozen=True, slots=True)
class SkippedFileEntry:
    path: str
    reason: str


@dataclass(frozen=True, slots=True)
class SkippedFilesResult:
    root_id: int
    root_path: str
    skipped_files: tuple[SkippedFileEntry, ...]


def list_skipped_files(
    *,
    session: Session,
    root_id: int | None = None,
    root_path: str | None = None,
) -> SkippedFilesResult:
    root = _resolve_root(session=session, root_id=root_id, root_path=root_path)
    built_manifest = build_root_dry_run_manifest(session=session, root_id=root.id)

    return SkippedFilesResult(
        root_id=root.id,
        root_path=root.path,
        skipped_files=tuple(
            SkippedFileEntry(
                path=decision.candidate.absolute_path,
                reason=decision.reason,
            )
            for decision in built_manifest.decisions
            if not decision.include
        ),
    )


def _resolve_root(
    *,
    session: Session,
    root_id: int | None,
    root_path: str | None,
) -> RootRecord:
    if (root_id is None) == (root_path is None):
        raise ValueError("Exactly one of root_id or root_path must be provided")

    repo = RootsRepository(session)
    if root_id is not None:
        root = repo.get_by_id(root_id)
        if root is None:
            raise LookupError(f"Root not found for id: {root_id}")
        return root

    resolved_root_path = resolve_path(root_path).as_posix()
    root = repo.get_by_path(resolved_root_path)
    if root is None:
        raise LookupError(f"Root not found for path: {resolved_root_path}")
    return root
