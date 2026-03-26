from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ProjectConfig
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootRecord, RootsRepository
from backup_projects.services.logging_setup import (
    RunLoggingConfig,
    configure_run_logging,
)
from backup_projects.services.manual_include_scan_service import (
    ManualIncludeScanResult,
    apply_manual_includes_for_root,
)
from backup_projects.services.project_dir_scan_service import (
    ProjectDirIncrementalScanResult,
    scan_and_sync_project_dir,
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


@dataclass(frozen=True, slots=True)
class ScanJobRootResult:
    root_id: int
    root_path: str
    status: str
    structural_rescan_performed: bool
    scanned_project_dir_count: int
    new_file_count: int
    changed_file_count: int
    reactivated_file_count: int
    missing_file_count: int
    processed_manual_include_count: int
    applied_manual_include_count: int
    error: str | None


@dataclass(frozen=True, slots=True)
class ScanJobLockedResult:
    run: RunLifecycleRecord
    lock_path: str


@dataclass(frozen=True, slots=True)
class ScanJobFinishedResult:
    run: RunLifecycleRecord
    roots: tuple[ScanJobRootResult, ...]
    summary: RunCountsSummary
    report: RunReportArtifacts
    log_file_path: str


@dataclass(slots=True)
class _RootAccumulator:
    root_id: int
    root_path: str
    status: str | None = None
    structural_rescan_performed: bool = False
    scanned_project_dir_count: int = 0
    new_file_count: int = 0
    changed_file_count: int = 0
    reactivated_file_count: int = 0
    missing_file_count: int = 0
    processed_manual_include_count: int = 0
    applied_manual_include_count: int = 0
    error: str | None = None

    def mark_structural_rescan_performed(self) -> None:
        self.structural_rescan_performed = True

    def add_project_dir_scan_result(self, result: ProjectDirIncrementalScanResult) -> None:
        self.scanned_project_dir_count += 1
        self.new_file_count += result.new_file_count
        self.changed_file_count += result.changed_file_count
        self.reactivated_file_count += result.reactivated_file_count
        self.missing_file_count += result.missing_file_count

    def add_manual_include_result(self, result: ManualIncludeScanResult) -> None:
        self.processed_manual_include_count += result.processed_include_count
        self.applied_manual_include_count += result.applied_include_count

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.error = error

    def to_result(self) -> ScanJobRootResult:
        return ScanJobRootResult(
            root_id=self.root_id,
            root_path=self.root_path,
            status=self.status or "completed",
            structural_rescan_performed=self.structural_rescan_performed,
            scanned_project_dir_count=self.scanned_project_dir_count,
            new_file_count=self.new_file_count,
            changed_file_count=self.changed_file_count,
            reactivated_file_count=self.reactivated_file_count,
            missing_file_count=self.missing_file_count,
            processed_manual_include_count=self.processed_manual_include_count,
            applied_manual_include_count=self.applied_manual_include_count,
            error=self.error,
        )


def run_scan_job(
    *,
    session: Session,
    config: ProjectConfig,
    trigger_mode: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> ScanJobFinishedResult | ScanJobLockedResult:
    run = start_run(
        session=session,
        run_type="scan",
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
        return ScanJobLockedResult(
            run=lock_result.run,
            lock_path=lock_result.lock_path,
        )

    with lock_result:
        try:
            logging_context = configure_run_logging(
                RunLoggingConfig(
                    run_id=run.id,
                    logs_dir=_resolve_runtime_dir(config, config.app_config.runtime.logs_dir),
                    logger_name=f"backup_projects.scan.run.{run.id}",
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
        roots_by_id: dict[int, _RootAccumulator] = {}
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
                roots_by_id[root.id] = _RootAccumulator(
                    root_id=root.id,
                    root_path=root.path,
                )

            logger.info("Scan root discovery completed")
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
                root_result = roots_by_id[root.id]
                if root_result.status == "failed" or not root.needs_structural_rescan:
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
                    _mark_root_failed(
                        session=session,
                        run_id=run.id,
                        root=root,
                        error=str(exc),
                        message=f"Root failed during structural rescan: {root.path}",
                        logger_message="Structural rescan failed for root %s",
                        logger=logger,
                        accumulator=root_result,
                        events=events,
                        now=now,
                    )
                    continue
                root_result.mark_structural_rescan_performed()
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
                root_result = roots_by_id[root.id]
                if root_result.status == "failed":
                    continue
                try:
                    for project_dir in project_dirs_repo.list_active_by_root(root.id):
                        root_result.add_project_dir_scan_result(
                            scan_and_sync_project_dir(
                                session=session,
                                project_dir_id=project_dir.id,
                                scanned_at=_format_timestamp(_resolve_now(now)),
                            )
                        )
                        scanned_project_dir_count += 1
                except Exception as exc:
                    project_dir_scan_failed += 1
                    _mark_root_failed(
                        session=session,
                        run_id=run.id,
                        root=root,
                        error=str(exc),
                        message=f"Root failed during project-dir scan: {root.path}",
                        logger_message="Project-dir scan failed for root %s",
                        logger=logger,
                        accumulator=root_result,
                        events=events,
                        now=now,
                    )

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

            processed_manual_include_count = 0
            applied_manual_include_count = 0
            manual_include_failed = 0
            for root in active_roots:
                root_result = roots_by_id[root.id]
                if root_result.status == "failed":
                    continue
                try:
                    manual_include_result = apply_manual_includes_for_root(
                        session=session,
                        root_id=root.id,
                        applied_at=_format_timestamp(_resolve_now(now)),
                    )
                except Exception as exc:
                    manual_include_failed += 1
                    _mark_root_failed(
                        session=session,
                        run_id=run.id,
                        root=root,
                        error=str(exc),
                        message=f"Root failed during manual-include apply: {root.path}",
                        logger_message="Manual includes failed for root %s",
                        logger=logger,
                        accumulator=root_result,
                        events=events,
                        now=now,
                    )
                    continue
                root_result.add_manual_include_result(manual_include_result)
                processed_manual_include_count += manual_include_result.processed_include_count
                applied_manual_include_count += manual_include_result.applied_include_count

            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="manual_includes_completed",
                    message="Manual-include phase completed",
                    payload={
                        "processed_include_count": processed_manual_include_count,
                        "applied_include_count": applied_manual_include_count,
                        "failed_root_count": manual_include_failed,
                    },
                    now=now,
                )
            )

            roots = tuple(roots_by_id[root.id].to_result() for root in active_roots)
            final_status = _compute_final_status(roots)
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
                    targets=_build_report_targets(roots),
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
                targets=_build_summary_targets(roots),
            )
            return ScanJobFinishedResult(
                run=finalized_run,
                roots=roots,
                summary=summary,
                report=report,
                log_file_path=str(logging_context.log_file_path),
            )
        except Exception as exc:
            logger.exception("Scan job failed")
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


def _mark_root_failed(
    *,
    session: Session,
    run_id: int,
    root: RootRecord,
    error: str,
    message: str,
    logger_message: str,
    logger,
    accumulator: _RootAccumulator,
    events: list[RunLifecycleEvent],
    now: Callable[[], datetime] | None,
) -> None:
    accumulator.mark_failed(error)
    events.append(
        append_run_event(
            session=session,
            run_id=run_id,
            event_type="scan_root_failed",
            message=message,
            payload={"root_id": root.id, "error": error},
            level="ERROR",
            now=now,
        )
    )
    logger.exception(logger_message, root.path)


def _build_summary_targets(
    roots: tuple[ScanJobRootResult, ...],
) -> tuple[RunSummaryTargetInput, ...]:
    return tuple(
        RunSummaryTargetInput(
            status=root.status,
            root_id=root.root_id,
            root_path=root.root_path,
        )
        for root in roots
    )


def _build_report_targets(
    roots: tuple[ScanJobRootResult, ...],
) -> tuple[RunReportTargetInput, ...]:
    return tuple(
        RunReportTargetInput(
            status=root.status,
            root_id=root.root_id,
            root_path=root.root_path,
            error=root.error,
        )
        for root in roots
    )


def _compute_final_status(roots: tuple[ScanJobRootResult, ...]) -> str:
    if any(root.status == "failed" for root in roots):
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
                event_type="scan_job_failed",
                message="Scan job failed",
                payload={"error": error},
                level="ERROR",
                now=now,
            )
        )
    except Exception:
        return
