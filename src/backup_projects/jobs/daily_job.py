from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.adapters.process.restic_runner import (
    ResticCommandFailureError,
    ResticTimeoutError,
)
from backup_projects.adapters.restic_adapter import (
    ResticOutputParseError,
    ResticSnapshotIdMissingError,
)
from backup_projects.config import ProjectConfig
from backup_projects.domain.models import ManifestResult
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootRecord, RootsRepository
from backup_projects.services.backup_service import (
    BackupServiceRequest,
    BackupServiceResult,
    run_backup_from_manifest,
)
from backup_projects.services.dry_run_service import build_multi_root_dry_run_manifest
from backup_projects.services.logging_setup import (
    RunLoggingConfig,
    configure_run_logging,
)
from backup_projects.services.manifest_builder import write_manifest
from backup_projects.services.manual_include_scan_service import (
    apply_manual_includes_for_root,
)
from backup_projects.services.post_backup_archive_service import (
    PostBackupArchiveRequest,
    run_post_backup_archive,
)
from backup_projects.services.project_dir_scan_service import scan_and_sync_project_dir
from backup_projects.services.report_delivery_service import (
    ReportDeliveryRequest,
    run_report_delivery,
)
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

_BACKUP_DIAGNOSTIC_EXCERPT_LIMIT = 500
_DEFAULT_RUN_BACKUP_FOR_ROOT = None


@dataclass(frozen=True, slots=True)
class DailyJobTargetResult:
    root_id: int
    root_path: str
    status: str
    included_count: int
    skipped_count: int
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
    manifest_result: ManifestResult | None
    backup_result: BackupServiceResult | None
    targets: tuple[DailyJobTargetResult, ...]
    summary: RunCountsSummary
    report: RunReportArtifacts
    log_file_path: str


@dataclass(slots=True)
class _TargetAccumulator:
    root_id: int
    root_path: str
    status: str | None = None
    included_count: int = 0
    skipped_count: int = 0
    manifest_result: ManifestResult | None = None
    backup_result: BackupServiceResult | None = None
    error: str | None = None

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.error = error

    def set_manifest_counts(self, *, included_count: int, skipped_count: int) -> None:
        self.included_count = included_count
        self.skipped_count = skipped_count

    def mark_completed(
        self,
        *,
        error: str | None = None,
    ) -> None:
        self.status = "completed"
        self.error = error

    def to_result(self) -> DailyJobTargetResult:
        status = self.status or "completed"
        return DailyJobTargetResult(
            root_id=self.root_id,
            root_path=self.root_path,
            status=status,
            included_count=self.included_count,
            skipped_count=self.skipped_count,
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

            run_manifest_result: ManifestResult | None = None
            run_backup_result: BackupServiceResult | None = None
            local_backup_failed = False
            post_backup_failed = False
            manifests_dir = _resolve_runtime_dir(config, config.app_config.runtime.manifests_dir)
            planned_roots = tuple(
                root for root in active_roots if targets_by_root[root.id].status != "failed"
            )
            manifest_plan = build_multi_root_dry_run_manifest(
                session=session,
                root_ids=tuple(root.id for root in planned_roots),
            )
            planned_roots_by_id = {root.id: root for root in planned_roots}

            for root_plan in manifest_plan.root_plans:
                target = targets_by_root[root_plan.root_id]
                if root_plan.status == "failed":
                    target.mark_failed(root_plan.error or "Failed to build root manifest")
                    root = planned_roots_by_id[root_plan.root_id]
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_root_failed",
                            message=f"Root failed during manifest planning: {root.path}",
                            payload={"root_id": root.id, "error": target.error},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.error(
                        "Manifest planning failed for root %s: %s",
                        root.path,
                        target.error,
                    )
                    continue

                target.set_manifest_counts(
                    included_count=root_plan.included_count,
                    skipped_count=root_plan.skipped_count,
                )

            backup_roots = tuple(
                root for root in planned_roots if targets_by_root[root.id].status != "failed"
            )
            if backup_roots:
                try:
                    if (
                        len(backup_roots) == 1
                        and _run_backup_for_root is not _DEFAULT_RUN_BACKUP_FOR_ROOT
                    ):
                        run_manifest_result, run_backup_result = _run_backup_for_root(
                            session=session,
                            root=backup_roots[0],
                            config=config,
                            manifests_dir=manifests_dir,
                            run_timestamp=run_timestamp,
                        )
                    else:
                        run_manifest_result = write_manifest(
                            built_manifest=manifest_plan.built_manifest,
                            output_dir=manifests_dir,
                            artifact_stem=_build_artifact_stem(
                                run_id=run.id,
                                run_timestamp=run_timestamp,
                            ),
                        )
                        run_backup_result = run_backup_from_manifest(
                            BackupServiceRequest(
                                manifest_result=run_manifest_result,
                                restic_binary=config.app_config.restic.binary,
                                restic_repository=config.app_config.restic.repository,
                                restic_password_env_var=config.app_config.restic.password_env_var,
                                restic_timeout_seconds=config.app_config.restic.timeout_seconds,
                            )
                        )
                except Exception as exc:
                    local_backup_failed = True
                    diagnostic = _build_backup_failure_diagnostic(exc)
                    for root in backup_roots:
                        targets_by_root[root.id].mark_failed(diagnostic.error_message)
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="daily_backup_failed",
                            message="Daily backup failed",
                            payload=diagnostic.event_payload,
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Daily backup failed%s", diagnostic.log_suffix)
                else:
                    for root in backup_roots:
                        targets_by_root[root.id].mark_completed()

                    if run_backup_result.restic_result is None:
                        events.append(
                            append_run_event(
                                session=session,
                                run_id=run.id,
                                event_type="daily_backup_skipped",
                                message="Daily backup skipped",
                                payload={
                                    "manifest_file_path": run_manifest_result.manifest_file_path,
                                    "message": run_backup_result.message,
                                },
                                now=now,
                            )
                        )
                        logger.info(
                            "Daily backup skipped: %s",
                            run_backup_result.message,
                        )
                    else:
                        events.append(
                            append_run_event(
                                session=session,
                                run_id=run.id,
                                event_type="daily_backup_completed",
                                message="Daily backup completed",
                                payload={
                                    "manifest_file_path": run_manifest_result.manifest_file_path,
                                    "snapshot_id": run_backup_result.restic_result.snapshot_id,
                                },
                                now=now,
                        )
                    )

            if backup_roots:
                post_backup_failed = _run_post_backup_archive_if_needed(
                    session=session,
                    run_id=run.id,
                    config=config,
                    run_backup_result=run_backup_result,
                    local_backup_failed=local_backup_failed,
                    events=events,
                    logger=logger,
                    now=now,
                )

            targets = tuple(targets_by_root[root.id].to_result() for root in active_roots)
            final_status = _compute_final_status(
                targets,
                post_backup_failed=post_backup_failed,
            )
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
                report = _write_daily_run_report(
                    config=config,
                    run=synthetic_final_run,
                    events=events,
                    targets=targets,
                    manifest_result=run_manifest_result,
                    backup_result=run_backup_result,
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

            _run_report_delivery_if_needed(
                session=session,
                run_id=run.id,
                config=config,
                report=report,
                events=events,
                logger=logger,
                now=now,
            )

            try:
                report = _write_daily_run_report(
                    config=config,
                    run=synthetic_final_run,
                    events=events,
                    targets=targets,
                    manifest_result=run_manifest_result,
                    backup_result=run_backup_result,
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
                backup_result=(
                    run_backup_result.restic_result
                    if run_backup_result is not None
                    and run_backup_result.restic_result is not None
                    else None
                ),
            )
            return DailyJobFinishedResult(
                run=finalized_run,
                manifest_result=run_manifest_result,
                backup_result=run_backup_result,
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


def _build_artifact_stem(*, run_id: int, run_timestamp: str) -> str:
    return f"daily-{run_timestamp}-run-{run_id}"


def _run_post_backup_archive_if_needed(
    *,
    session: Session,
    run_id: int,
    config: ProjectConfig,
    run_backup_result: BackupServiceResult | None,
    local_backup_failed: bool,
    events: list[RunLifecycleEvent],
    logger,
    now: Callable[[], datetime] | None,
) -> bool:
    if local_backup_failed:
        _append_archive_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            reason="backup_failed",
            now=now,
        )
        _append_local_retention_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            keep_last=config.app_config.restic.archive.local_retention_keep_last,
            reason="backup_failed",
            now=now,
        )
        logger.info("Daily archive skipped because local backup failed")
        logger.info("Daily local retention skipped because local backup failed")
        return False

    restic_result = (
        run_backup_result.restic_result if run_backup_result is not None else None
    )
    if restic_result is None:
        _append_archive_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            reason="no_snapshot",
            now=now,
        )
        _append_local_retention_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            keep_last=config.app_config.restic.archive.local_retention_keep_last,
            reason="no_snapshot",
            now=now,
        )
        logger.info("Daily archive skipped because no snapshot was produced")
        logger.info("Daily local retention skipped because no snapshot was produced")
        return False

    archive_config = config.app_config.restic.archive
    if not archive_config.enabled:
        _append_archive_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            reason="archive_disabled",
            now=now,
        )
        _append_local_retention_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            keep_last=archive_config.local_retention_keep_last,
            reason="archive_disabled",
            now=now,
        )
        logger.info("Daily archive skipped because archive is disabled in config")
        logger.info("Daily local retention skipped because archive is disabled in config")
        return False

    snapshot_id = restic_result.snapshot_id
    remote_repository = archive_config.remote_repository or ""
    keep_last = archive_config.local_retention_keep_last

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_archive_started",
            message="Daily archive started",
            payload={
                "snapshot_id": snapshot_id,
                "remote_repository": remote_repository,
            },
            now=now,
        )
    )
    logger.info(
        "Starting daily archive for snapshot %s to %s",
        snapshot_id,
        remote_repository,
    )

    archive_result = run_post_backup_archive(
        PostBackupArchiveRequest(
            snapshot_id=snapshot_id,
            restic_binary=config.app_config.restic.binary,
            local_repository=config.app_config.restic.repository,
            local_password_env_var=config.app_config.restic.password_env_var,
            remote_repository=remote_repository,
            remote_password_env_var=archive_config.remote_password_env_var or "",
            timeout_seconds=config.app_config.restic.timeout_seconds,
            local_retention_keep_last=keep_last,
        )
    )

    if archive_result.archive_status == "failed":
        payload = {
            "snapshot_id": snapshot_id,
            "remote_repository": remote_repository,
            "error": archive_result.archive_error,
        }
        diagnostic = None
        if archive_result.archive_exception is not None:
            diagnostic = _build_post_backup_failure_diagnostic(
                archive_result.archive_exception
            )
            payload.update(diagnostic.event_payload)

        events.append(
            append_run_event(
                session=session,
                run_id=run_id,
                event_type="daily_archive_failed",
                message="Daily archive failed",
                payload=payload,
                level="ERROR",
                now=now,
            )
        )
        _append_local_retention_skipped_event(
            session=session,
            run_id=run_id,
            events=events,
            keep_last=keep_last,
            reason="archive_failed",
            now=now,
        )
        logger.error(
            "Daily archive failed: %s%s",
            archive_result.archive_error,
            diagnostic.log_suffix if diagnostic is not None else "",
        )
        logger.info("Daily local retention skipped because archive failed")
        return True

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_archive_completed",
            message="Daily archive completed",
            payload={
                "snapshot_id": snapshot_id,
                "remote_repository": remote_repository,
            },
            now=now,
        )
    )
    logger.info(
        "Daily archive completed for snapshot %s to %s",
        snapshot_id,
        remote_repository,
    )

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_local_retention_started",
            message="Daily local retention started",
            payload={
                "snapshot_id": snapshot_id,
                "keep_last": keep_last,
            },
            now=now,
        )
    )
    logger.info(
        "Starting daily local retention for snapshot %s with keep-last=%s",
        snapshot_id,
        keep_last,
    )

    if archive_result.retention_status == "failed":
        payload = {
            "snapshot_id": snapshot_id,
            "keep_last": keep_last,
            "error": archive_result.retention_error,
        }
        diagnostic = None
        if archive_result.retention_exception is not None:
            diagnostic = _build_post_backup_failure_diagnostic(
                archive_result.retention_exception
            )
            payload.update(diagnostic.event_payload)

        events.append(
            append_run_event(
                session=session,
                run_id=run_id,
                event_type="daily_local_retention_failed",
                message="Daily local retention failed",
                payload=payload,
                level="ERROR",
                now=now,
            )
        )
        logger.error(
            "Daily local retention failed: %s%s",
            archive_result.retention_error,
            diagnostic.log_suffix if diagnostic is not None else "",
        )
        return True

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_local_retention_completed",
            message="Daily local retention completed",
            payload={
                "snapshot_id": snapshot_id,
                "keep_last": keep_last,
            },
            now=now,
        )
    )
    logger.info(
        "Daily local retention completed for snapshot %s with keep-last=%s",
        snapshot_id,
        keep_last,
    )
    return False


def _append_archive_skipped_event(
    *,
    session: Session,
    run_id: int,
    events: list[RunLifecycleEvent],
    reason: str,
    now: Callable[[], datetime] | None,
) -> None:
    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_archive_skipped",
            message="Daily archive skipped",
            payload={"reason": reason},
            now=now,
        )
    )


def _append_local_retention_skipped_event(
    *,
    session: Session,
    run_id: int,
    events: list[RunLifecycleEvent],
    keep_last: int,
    reason: str,
    now: Callable[[], datetime] | None,
) -> None:
    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_local_retention_skipped",
            message="Daily local retention skipped",
            payload={"reason": reason, "keep_last": keep_last},
            now=now,
        )
    )


def _run_report_delivery_if_needed(
    *,
    session: Session,
    run_id: int,
    config: ProjectConfig,
    report: RunReportArtifacts,
    events: list[RunLifecycleEvent],
    logger,
    now: Callable[[], datetime] | None,
) -> None:
    delivery_config = config.app_config.report_delivery
    if not delivery_config.enabled:
        events.append(
            append_run_event(
                session=session,
                run_id=run_id,
                event_type="daily_report_delivery_skipped",
                message="Daily report delivery skipped",
                payload={"reason": "delivery_disabled"},
                now=now,
            )
        )
        logger.info("Daily report delivery skipped because report delivery is disabled")
        return

    source_report_path = report.text_report_path
    output_dir = _resolve_runtime_dir(config, delivery_config.output_dir or "")
    mode = delivery_config.mode or "local_file"

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_report_delivery_started",
            message="Daily report delivery started",
            payload={
                "mode": mode,
                "source_report_path": source_report_path,
                "output_dir": str(output_dir),
            },
            now=now,
        )
    )
    logger.info(
        "Starting daily report delivery: mode=%s source=%s output_dir=%s",
        mode,
        source_report_path,
        output_dir,
    )

    delivery_result = run_report_delivery(
        ReportDeliveryRequest(
            run_id=run_id,
            mode=mode,
            source_report_path=source_report_path,
            output_dir=str(output_dir),
        )
    )

    if delivery_result.status == "completed":
        events.append(
            append_run_event(
                session=session,
                run_id=run_id,
                event_type="daily_report_delivery_completed",
                message="Daily report delivery completed",
                payload={
                    "mode": mode,
                    "source_report_path": source_report_path,
                    "destination_path": delivery_result.destination_path,
                },
                now=now,
            )
        )
        logger.info(
            "Daily report delivery completed: destination=%s",
            delivery_result.destination_path,
        )
        return

    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="daily_report_delivery_failed",
            message="Daily report delivery failed",
            payload={
                "mode": mode,
                "source_report_path": source_report_path,
                "destination_path": delivery_result.destination_path,
                "error": delivery_result.error,
            },
            level="ERROR",
            now=now,
        )
    )
    logger.error(
        "Daily report delivery failed: %s",
        delivery_result.error,
    )


def _run_backup_for_root(
    *,
    session: Session,
    root: RootRecord,
    config: ProjectConfig,
    manifests_dir: Path,
    run_timestamp: str,
):
    built_manifest = build_multi_root_dry_run_manifest(
        session=session,
        root_ids=(root.id,),
    ).built_manifest
    manifest_result = write_manifest(
        built_manifest=built_manifest,
        output_dir=manifests_dir,
        artifact_stem=f"daily-{run_timestamp}-root-{root.id}",
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


_DEFAULT_RUN_BACKUP_FOR_ROOT = _run_backup_for_root


def _build_summary_targets(
    targets: tuple[DailyJobTargetResult, ...],
) -> tuple[RunSummaryTargetInput, ...]:
    return tuple(
        RunSummaryTargetInput(
            status=target.status,
            root_id=target.root_id,
            root_path=target.root_path,
            included_count=target.included_count,
            skipped_count=target.skipped_count,
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
            included_count=target.included_count,
            skipped_count=target.skipped_count,
            error=target.error,
        )
        for target in targets
    )


def _write_daily_run_report(
    *,
    config: ProjectConfig,
    run: RunLifecycleRecord,
    events: list[RunLifecycleEvent],
    targets: tuple[DailyJobTargetResult, ...],
    manifest_result: ManifestResult | None,
    backup_result: BackupServiceResult | None,
) -> RunReportArtifacts:
    return write_run_report(
        reports_dir=_resolve_runtime_dir(
            config,
            config.app_config.runtime.reports_dir,
        ),
        run=run,
        events=events,
        manifest_result=manifest_result,
        backup_result=(
            backup_result.restic_result
            if backup_result is not None and backup_result.restic_result is not None
            else None
        ),
        targets=_build_report_targets(targets),
    )


def _compute_final_status(
    targets: tuple[DailyJobTargetResult, ...],
    *,
    post_backup_failed: bool = False,
) -> str:
    if post_backup_failed or any(target.status == "failed" for target in targets):
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


@dataclass(frozen=True, slots=True)
class _BackupFailureDiagnostic:
    error_message: str
    event_payload: dict[str, object]
    log_suffix: str


def _build_backup_failure_diagnostic(exc: Exception) -> _BackupFailureDiagnostic:
    event_payload: dict[str, object] = {"error": str(exc)}
    details: list[str] = []

    if isinstance(
        exc,
        (
            ResticCommandFailureError,
            ResticTimeoutError,
            ResticOutputParseError,
            ResticSnapshotIdMissingError,
        ),
    ):
        argv = getattr(exc, "argv", None)
        if argv is not None:
            argv_list = [str(part) for part in argv]
            event_payload["argv"] = argv_list
            details.append(f"argv={argv_list!r}")

        returncode = getattr(exc, "returncode", None)
        if isinstance(returncode, int):
            event_payload["returncode"] = returncode
            details.append(f"returncode={returncode}")

        timeout_seconds = getattr(exc, "timeout_seconds", None)
        if isinstance(timeout_seconds, (int, float)):
            event_payload["timeout_seconds"] = timeout_seconds
            details.append(f"timeout_seconds={timeout_seconds}")

        stderr_excerpt = _make_excerpt(getattr(exc, "stderr", None))
        if stderr_excerpt is not None:
            event_payload["stderr_excerpt"] = stderr_excerpt
            details.append(f"stderr={stderr_excerpt!r}")

        stdout_excerpt = _make_excerpt(getattr(exc, "stdout", None))
        if stdout_excerpt is not None:
            event_payload["stdout_excerpt"] = stdout_excerpt
            details.append(f"stdout={stdout_excerpt!r}")

    detail_suffix = ""
    if details:
        detail_suffix = " | " + " | ".join(details)

    return _BackupFailureDiagnostic(
        error_message=f"{str(exc)}{detail_suffix}",
        event_payload=event_payload,
        log_suffix=detail_suffix,
    )


def _build_post_backup_failure_diagnostic(exc: Exception) -> _BackupFailureDiagnostic:
    event_payload: dict[str, object] = {"error": str(exc)}
    details: list[str] = []

    if isinstance(
        exc,
        (
            ResticCommandFailureError,
            ResticTimeoutError,
            ResticOutputParseError,
            ResticSnapshotIdMissingError,
        ),
    ):
        argv = getattr(exc, "argv", None)
        if argv is not None:
            argv_list = [str(part) for part in argv]
            event_payload["argv"] = argv_list
            details.append(f"argv={argv_list!r}")

        returncode = getattr(exc, "returncode", None)
        if isinstance(returncode, int):
            event_payload["returncode"] = returncode
            details.append(f"returncode={returncode}")

        timeout_seconds = getattr(exc, "timeout_seconds", None)
        if isinstance(timeout_seconds, (int, float)):
            event_payload["timeout_seconds"] = timeout_seconds
            details.append(f"timeout_seconds={timeout_seconds}")

        stderr_excerpt = _make_excerpt(getattr(exc, "stderr", None))
        if stderr_excerpt is not None:
            event_payload["stderr_excerpt"] = stderr_excerpt
            details.append(f"stderr={stderr_excerpt!r}")

        stdout_excerpt = _make_excerpt(getattr(exc, "stdout", None))
        if stdout_excerpt is not None:
            event_payload["stdout_excerpt"] = stdout_excerpt
            details.append(f"stdout={stdout_excerpt!r}")

    detail_suffix = ""
    if details:
        detail_suffix = " | " + " | ".join(details)

    return _BackupFailureDiagnostic(
        error_message=f"{str(exc)}{detail_suffix}",
        event_payload=event_payload,
        log_suffix=detail_suffix,
    )


def _make_excerpt(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if normalized == "":
        return None
    if len(normalized) <= _BACKUP_DIAGNOSTIC_EXCERPT_LIMIT:
        return normalized
    return normalized[:_BACKUP_DIAGNOSTIC_EXCERPT_LIMIT] + "...<truncated>"
