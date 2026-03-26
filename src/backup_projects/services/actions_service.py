from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from backup_projects.config import ProjectConfig
from backup_projects.jobs.backup_job import BackupJobLockedResult, run_backup_job
from backup_projects.jobs.daily_job import DailyJobLockedResult, run_daily_job
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.dry_run_service import build_root_dry_run_manifest
from backup_projects.services.run_lock import (
    RunLockUnavailable,
    try_acquire_run_lock_without_run,
)
from backup_projects.services.structural_scan_service import scan_root_structure
from backup_projects.services.structural_scan_sync_service import sync_structural_scan_result


@dataclass(frozen=True, slots=True)
class ActionResultField:
    label: str
    value: str


@dataclass(frozen=True, slots=True)
class ActionResult:
    action_name: str
    status: str
    message: str
    fields: tuple[ActionResultField, ...] = ()
    details_text: str | None = None
    back_href: str | None = None
    back_label: str | None = None


def run_daily_now(
    *,
    session: Session,
    config: ProjectConfig,
) -> ActionResult:
    result = run_daily_job(
        session=session,
        config=config,
        trigger_mode="manual",
    )

    if isinstance(result, DailyJobLockedResult):
        return ActionResult(
            action_name="Run daily now",
            status="locked",
            message="Daily run could not start because another run is active.",
            fields=(
                ActionResultField(label="Run id", value=str(result.run.id)),
                ActionResultField(label="Lock path", value=result.lock_path),
            ),
            back_href="/",
            back_label="Back to dashboard",
        )

    return ActionResult(
        action_name="Run daily now",
        status=result.run.status,
        message=(
            "Daily run completed successfully."
            if result.run.status == "completed"
            else "Daily run finished with failures."
        ),
        fields=(
            ActionResultField(label="Run id", value=str(result.run.id)),
            ActionResultField(label="Targets total", value=str(result.summary.targets_total)),
            ActionResultField(
                label="Targets succeeded",
                value=str(result.summary.targets_succeeded),
            ),
            ActionResultField(
                label="Targets failed",
                value=str(result.summary.targets_failed),
            ),
        ),
        back_href="/",
        back_label="Back to dashboard",
    )


def run_backup_now(
    *,
    session: Session,
    config: ProjectConfig,
) -> ActionResult:
    result = run_backup_job(
        session=session,
        config=config,
        trigger_mode="manual",
    )

    if isinstance(result, BackupJobLockedResult):
        return ActionResult(
            action_name="Backup now",
            status="locked",
            message="Backup run could not start because another run is active.",
            fields=(
                ActionResultField(label="Run id", value=str(result.run.id)),
                ActionResultField(label="Lock path", value=result.lock_path),
            ),
            back_href="/",
            back_label="Back to dashboard",
        )

    return ActionResult(
        action_name="Backup now",
        status=result.run.status,
        message=(
            "Backup run completed successfully."
            if result.run.status == "completed"
            else "Backup run finished with failures."
        ),
        fields=(
            ActionResultField(label="Run id", value=str(result.run.id)),
            ActionResultField(label="Roots total", value=str(result.summary.targets_total)),
            ActionResultField(
                label="Roots succeeded",
                value=str(result.summary.targets_succeeded),
            ),
            ActionResultField(
                label="Roots failed",
                value=str(result.summary.targets_failed),
            ),
        ),
        back_href="/",
        back_label="Back to dashboard",
    )


def dry_run_root_now(
    *,
    session: Session,
    root_id: int,
) -> ActionResult:
    try:
        root = _require_root(session=session, root_id=root_id)
        built_manifest = build_root_dry_run_manifest(session=session, root_id=root_id)
    except Exception as exc:
        return ActionResult(
            action_name="Dry-run now",
            status="failed",
            message=f"Dry run failed for root {root_id}: {exc}",
            back_href="/roots",
            back_label="Back to roots",
        )

    counts_payload = built_manifest.json_payload.get("counts")
    included_count = "-"
    skipped_count = "-"
    if isinstance(counts_payload, dict):
        included_value = counts_payload.get("included")
        skipped_value = counts_payload.get("skipped")
        if isinstance(included_value, int) and not isinstance(included_value, bool):
            included_count = str(included_value)
        if isinstance(skipped_value, int) and not isinstance(skipped_value, bool):
            skipped_count = str(skipped_value)

    return ActionResult(
        action_name="Dry-run now",
        status="completed",
        message=f"Dry run completed for root: {root.path}",
        fields=(
            ActionResultField(label="Root id", value=str(root.id)),
            ActionResultField(label="Root path", value=root.path),
            ActionResultField(label="Included", value=included_count),
            ActionResultField(label="Skipped", value=skipped_count),
        ),
        details_text=built_manifest.summary_text,
        back_href="/roots",
        back_label="Back to roots",
    )


def rescan_root_now(
    *,
    session: Session,
    config: ProjectConfig,
    root_id: int,
    now: Callable[[], datetime] | None = None,
) -> ActionResult:
    root = _require_root(session=session, root_id=root_id)
    lock_result = try_acquire_run_lock_without_run(
        locks_dir=_resolve_runtime_dir(config, config.app_config.runtime.locks_dir),
    )
    if isinstance(lock_result, RunLockUnavailable):
        return ActionResult(
            action_name="Rescan root",
            status="locked",
            message="Root rescan could not start because another run is active.",
            fields=(
                ActionResultField(label="Lock path", value=lock_result.lock_path),
                ActionResultField(label="Root id", value=str(root.id)),
            ),
            back_href="/roots",
            back_label="Back to roots",
        )

    with lock_result:
        scan_result = scan_root_structure(
            root_path=root.path,
            allowed_extensions=config.rules_config.allowed_extensions,
        )
        sync_result = sync_structural_scan_result(
            session=session,
            root_id=root.id,
            scan_result=scan_result,
            synced_at=_format_timestamp(_resolve_now(now)),
        )
        RootsRepository(session).mark_present(
            root.id,
            device_id=root.device_id,
            inode=root.inode,
            mtime_ns=root.mtime_ns,
            ctime_ns=root.ctime_ns,
            last_seen_at=root.last_seen_at,
            needs_structural_rescan=False,
        )

    return ActionResult(
        action_name="Rescan root",
        status="completed",
        message=f"Root rescan completed for: {root.path}",
        fields=(
            ActionResultField(label="Root id", value=str(root.id)),
            ActionResultField(
                label="Scanned project dirs",
                value=str(sync_result.scanned_project_dir_count),
            ),
            ActionResultField(
                label="Scanned project files",
                value=str(sync_result.scanned_project_file_count),
            ),
        ),
        back_href="/roots",
        back_label="Back to roots",
    )


def _require_root(*, session: Session, root_id: int):
    root = RootsRepository(session).get_by_id(root_id)
    if root is None:
        raise LookupError(f"Root not found for id: {root_id}")
    return root


def _resolve_now(now: Callable[[], datetime] | None) -> datetime:
    return datetime.now(timezone.utc) if now is None else now()


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _resolve_runtime_dir(config: ProjectConfig, runtime_path: str):
    from backup_projects.adapters.filesystem.path_utils import resolve_path

    return resolve_path(config.app_path.parent / runtime_path)
