from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ProjectConfig
from backup_projects.domain.models import ManifestResult
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootRecord, RootsRepository
from backup_projects.services.backup_service import (
    BackupServiceRequest,
    BackupServiceResult,
    run_backup_from_manifest,
)
from backup_projects.services.dry_run_service import build_root_dry_run_manifest
from backup_projects.services.logging_setup import (
    RunLoggingConfig,
    configure_run_logging,
)
from backup_projects.services.manifest_builder import write_manifest
from backup_projects.services.manual_include_scan_service import (
    apply_manual_includes_for_root,
)
from backup_projects.services.project_dir_scan_service import scan_and_sync_project_dir
from backup_projects.services.report_service import (
    RunReportArtifacts,
    RunReportTargetInput,
    write_run_report,
)
from backup_projects.services.root_discovery_service import discover_and_sync_roots
from backup_projects.services.run_lock import RunLockDenied, try_acquire_run_lock
from backup_projects.services.run_service import (
    RunLifecycleEvent,
    RunLifecycleRecord,
    append_run_event,
    finish_run,
    start_run,
)
from backup_projects.services.structural_scan_service import scan_root_structure
from backup_projects.services.structural_scan_sync_service import (
    sync_structural_scan_result,
)
from backup_projects.services.summary_service import (
    RunCountsSummary,
    RunSummaryTargetInput,
    build_run_summary,
)


@dataclass(frozen=True, slots=True)
class DailyJobTargetResult:
    root_id: int
    root_path: str
    status: str
    manifest_result: ManifestResult | None
    backup_result: BackupServiceResult | None
    error: str | None


@dataclass(frozen=True, slots=True)
class DailyJobLockedResult:
    run: RunLifecycleRecord
    lock_path: str


@dataclass(frozen=True, slots=True)
class DailyJobFinishedResult:
    run: RunLifecycleRecord
    targets: tuple[DailyJobTargetResult, ...]
    summary: RunCountsSummary
    report: RunReportArtifacts
    log_file_path: str


@dataclass(slots=True)
class _TargetAccumulator:
    root_id: int
    root_path: str
    status: str | None = None
    manifest_result: ManifestResult | None = None
    backup_result: BackupServiceResult | None = None
    error: str | None = None

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.error = error

    def mark_completed(self, *, manifest_result, backup_result: BackupServiceResult) -> None:
        self.status = "completed"
        self.manifest_result = manifest_result
        self.backup_result = backup_result
        self.error = None

    def to_result(self) -> DailyJobTargetResult:
        status = self.status or "completed"
        return DailyJobTargetResult(
            root_id=self.root_id,
            root_path=self.root_path,
            status=status,
            manifest_result=self.manifest_result,
            backup_result=self.backup_result,
            error=self.error,
        )


def run_daily_job(
    *,
    session: Session,
    config: ProjectConfig,
    trigger_mode: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> DailyJobFinishedResult | DailyJobLockedResult:
    run = start_run(
        session=session,
        run_type="daily",
        trigger_mode=trigger_mode or config.app_config.scheduler.mode,
        now=now,
    )
    try:
        lock_result = try_acquire_run_lock(
            session=session,
            run_id=run.id,
            locks_dir=_resolve_runtime_dir(config, config.app_config.runtime.locks_dir),
            now=now,
        )
    except Exception:
        finish_run(
            session=session,
            run_id=run.id,
            status="failed",
            now=now,
        )
        raise
    if isinstance(lock_result, RunLockDenied):
        return DailyJobLockedResult(
            run=lock_result.run,
            lock_path=lock_result.lock_path,
        )

    with lock_result:
        try:
            logging_context = configure_run_logging(
                RunLoggingConfig(
                    run_id=run.id,
                    logs_dir=_resolve_runtime_dir(config, config.app_config.runtime.logs_dir),
                    logger_name=f"backup_projects.daily.run.{run.id}",
                    console_level=config.app_config.app.log_level,
                    file_level=config.app_config.app.log_level,
                )
            )
        except Exception:
            finish_run(
                session=session,
                run_id=run.id,
                status="failed",
                now=now,
            )
            raise
        logger = logging_context.logger
        events: list[RunLifecycleEvent] = []
        targets_by_root: dict[int, _TargetAccumulator] = {}
        run_timestamp = _format_artifact_timestamp(_resolve_now(now))
        run_finalized = False

        try:
            enabled_raid_roots = tuple(
                raid_root for raid_root in config.app_config.raid_roots if raid_root.enabled
            )
            enabled_raid_names = {raid_root.name for raid_root in enabled_raid_roots}

            discovered_root_count = 0
            for raid_root in enabled_raid_roots:
                discovery_result = discover_and_sync_roots(
                    session=session,
                    raid_name=raid_root.name,
                    raid_path=raid_root.path,
                    discovered_at=_format_timestamp(_resolve_now(now)),
                )
                discovered_root_count += len(discovery_result.discovered)

            active_roots = tuple(
                root
                for root in RootsRepository(session).list_active()
                if root.raid_name in enabled_raid_names
            )
            for root in active_roots:
                targets_by_root[root.id] = _TargetAccumulator(
                    root_id=root.id,
                    root_path=root.path,
                )

            logger.info("Daily root discovery completed", extra={})
            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="roots_discovered",
                    message="Root discovery completed",
                    payload={
                        "raid_targets": len(enabled_raid_roots),
                        "discovered_root_count": discovered_root_count,
                        "active_root_count": len(active_roots),
                    },
                    now=now,
                )
            )

            structural_rescanned = 0
            structural_failed = 0
            for root in active_roots:
                target = targets_by_root[root.id]
                if target.status == "failed" or not root.needs_structural_rescan:
                    continue
                try:
                    _run_structural_rescan_for_root(
                        session=session,
                        root=root,
                        config=config,
                        now=now,
                    )
                except Exception as exc:
                    structural_failed += 1
                    target.mark_failed(str(exc))
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_root_failed",
                            message=f"Root failed during structural rescan: {root.path}",
                            payload={"root_id": root.id, "error": str(exc)},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Structural rescan failed for root %s", root.path)
                    continue
                structural_rescanned += 1

            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="structural_rescan_completed",
                    message="Structural rescan phase completed",
                    payload={
                        "rescanned_root_count": structural_rescanned,
                        "failed_root_count": structural_failed,
                    },
                    now=now,
                )
            )

            scanned_project_dir_count = 0
            project_dir_scan_failed = 0
            project_dirs_repo = ProjectDirsRepository(session)
            for root in active_roots:
                target = targets_by_root[root.id]
                if target.status == "failed":
                    continue
                try:
                    for project_dir in project_dirs_repo.list_active_by_root(root.id):
                        scan_and_sync_project_dir(
                            session=session,
                            project_dir_id=project_dir.id,
                            scanned_at=_format_timestamp(_resolve_now(now)),
                        )
                        scanned_project_dir_count += 1
                except Exception as exc:
                    project_dir_scan_failed += 1
                    target.mark_failed(str(exc))
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_root_failed",
                            message=f"Root failed during project-dir scan: {root.path}",
                            payload={"root_id": root.id, "error": str(exc)},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Project-dir scan failed for root %s", root.path)

            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="project_dir_scan_completed",
                    message="Project-dir scan phase completed",
                    payload={
                        "scanned_project_dir_count": scanned_project_dir_count,
                        "failed_root_count": project_dir_scan_failed,
                    },
                    now=now,
                )
            )

            applied_manual_include_count = 0
            manual_include_failed = 0
            for root in active_roots:
                target = targets_by_root[root.id]
                if target.status == "failed":
                    continue
                try:
                    result = apply_manual_includes_for_root(
                        session=session,
                        root_id=root.id,
                        applied_at=_format_timestamp(_resolve_now(now)),
                    )
                except Exception as exc:
                    manual_include_failed += 1
                    target.mark_failed(str(exc))
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_root_failed",
                            message=f"Root failed during manual-include apply: {root.path}",
                            payload={"root_id": root.id, "error": str(exc)},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Manual includes failed for root %s", root.path)
                    continue
                applied_manual_include_count += result.applied_include_count

            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="manual_includes_completed",
                    message="Manual-include phase completed",
                    payload={
                        "applied_include_count": applied_manual_include_count,
                        "failed_root_count": manual_include_failed,
                    },
                    now=now,
                )
            )

            manifests_dir = _resolve_runtime_dir(config, config.app_config.runtime.manifests_dir)
            for root in active_roots:
                target = targets_by_root[root.id]
                if target.status == "failed":
                    continue

                try:
                    manifest_result, backup_result = _run_backup_for_root(
                        session=session,
                        root=root,
                        config=config,
                        manifests_dir=manifests_dir,
                        run_timestamp=run_timestamp,
                    )
                except Exception as exc:
                    target.mark_failed(str(exc))
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_root_failed",
                            message=f"Daily backup failed for root: {root.path}",
                            payload={"root_id": root.id, "error": str(exc)},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Daily backup failed for root %s", root.path)
                    continue

                target.mark_completed(
                    manifest_result=manifest_result,
                    backup_result=backup_result,
                )
                events.append(
                    append_run_event(
                        session=session,
                        run_id=run.id,
                        event_type="daily_root_completed",
                        message=f"Daily backup completed for root: {root.path}",
                        payload={
                            "root_id": root.id,
                            "snapshot_id": backup_result.restic_result.snapshot_id,
                        },
                        now=now,
                    )
                )

            targets = tuple(targets_by_root[root.id].to_result() for root in active_roots)
            final_status = _compute_final_status(targets)
            finished_at_value = _resolve_now(now)
            synthetic_final_run = RunLifecycleRecord(
                id=run.id,
                run_type=run.run_type,
                status=final_status,
                started_at=run.started_at,
                trigger_mode=run.trigger_mode,
                finished_at=_format_timestamp(finished_at_value),
            )

            try:
                report = write_run_report(
                    reports_dir=_resolve_runtime_dir(
                        config,
                        config.app_config.runtime.reports_dir,
                    ),
                    run=synthetic_final_run,
                    events=events,
                    targets=_build_report_targets(targets),
                )
            except Exception as exc:
                _try_append_failed_job_event(
                    session=session,
                    run_id=run.id,
                    error=str(exc),
                    events=events,
                    now=now,
                )
                finish_run(
                    session=session,
                    run_id=run.id,
                    status="failed",
                    now=lambda: finished_at_value,
                )
                run_finalized = True
                raise

            finalized_run = finish_run(
                session=session,
                run_id=run.id,
                status=final_status,
                now=lambda: finished_at_value,
            )
            run_finalized = True
            summary = build_run_summary(
                run=finalized_run,
                targets=_build_summary_targets(targets),
            )
            return DailyJobFinishedResult(
                run=finalized_run,
                targets=targets,
                summary=summary,
                report=report,
                log_file_path=str(logging_context.log_file_path),
            )
        except Exception as exc:
            logger.exception("Daily job failed")
            if not run_finalized:
                _try_append_failed_job_event(
                    session=session,
                    run_id=run.id,
                    error=str(exc),
                    events=events,
                    now=now,
                )
                finish_run(
                    session=session,
                    run_id=run.id,
                    status="failed",
                    now=now,
                )
            raise


def _resolve_runtime_dir(config: ProjectConfig, runtime_path: str) -> Path:
    return resolve_path(config.app_path.parent / runtime_path)


def _resolve_now(now: Callable[[], datetime] | None) -> datetime:
    return datetime.now(timezone.utc) if now is None else now()


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _format_artifact_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _run_structural_rescan_for_root(
    *,
    session: Session,
    root: RootRecord,
    config: ProjectConfig,
    now: Callable[[], datetime] | None,
) -> None:
    scan_result = scan_root_structure(
        root_path=root.path,
        allowed_extensions=config.rules_config.allowed_extensions,
    )
    sync_structural_scan_result(
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


def _run_backup_for_root(
    *,
    session: Session,
    root: RootRecord,
    config: ProjectConfig,
    manifests_dir: Path,
    run_timestamp: str,
):
    built_manifest = build_root_dry_run_manifest(
        session=session,
        root_id=root.id,
    )
    manifest_result = write_manifest(
        built_manifest=built_manifest,
        output_dir=manifests_dir,
        artifact_stem=_build_artifact_stem(
            root_id=root.id,
            run_timestamp=run_timestamp,
        ),
    )
    backup_result = run_backup_from_manifest(
        BackupServiceRequest(
            manifest_result=manifest_result,
            restic_binary=config.app_config.restic.binary,
            restic_repository=config.app_config.restic.repository,
            restic_password_env_var=config.app_config.restic.password_env_var,
            restic_timeout_seconds=config.app_config.restic.timeout_seconds,
        )
    )
    return manifest_result, backup_result


def _build_artifact_stem(*, root_id: int, run_timestamp: str) -> str:
    return f"daily-{run_timestamp}-root-{root_id}"


def _build_summary_targets(
    targets: tuple[DailyJobTargetResult, ...],
) -> tuple[RunSummaryTargetInput, ...]:
    return tuple(
        RunSummaryTargetInput(
            status=target.status,
            root_id=target.root_id,
            root_path=target.root_path,
            manifest_result=target.manifest_result,
            backup_result=(
                target.backup_result.restic_result
                if target.backup_result is not None
                else None
            ),
        )
        for target in targets
    )


def _build_report_targets(
    targets: tuple[DailyJobTargetResult, ...],
) -> tuple[RunReportTargetInput, ...]:
    return tuple(
        RunReportTargetInput(
            status=target.status,
            root_id=target.root_id,
            root_path=target.root_path,
            manifest_result=target.manifest_result,
            backup_result=(
                target.backup_result.restic_result
                if target.backup_result is not None
                else None
            ),
            error=target.error,
        )
        for target in targets
    )


def _compute_final_status(targets: tuple[DailyJobTargetResult, ...]) -> str:
    if any(target.status == "failed" for target in targets):
        return "failed"
    return "completed"


def _try_append_failed_job_event(
    *,
    session: Session,
    run_id: int,
    error: str,
    events: list[RunLifecycleEvent],
    now: Callable[[], datetime] | None,
) -> None:
    try:
        events.append(
            append_run_event(
                session=session,
                run_id=run_id,
                event_type="daily_job_failed",
                message="Daily job failed",
                payload={"error": error},
                level="ERROR",
                now=now,
            )
        )
    except Exception:
        return
