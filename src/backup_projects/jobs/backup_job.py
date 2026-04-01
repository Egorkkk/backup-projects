from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ProjectConfig
from backup_projects.domain.models import ManifestResult
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
from backup_projects.services.report_service import (
    RunReportArtifacts,
    RunReportTargetInput,
    write_run_report,
)
from backup_projects.services.run_lock import RunLockDenied, try_acquire_run_lock
from backup_projects.services.run_service import (
    RunLifecycleEvent,
    RunLifecycleRecord,
    append_run_event,
    finish_run,
    start_run,
)
from backup_projects.services.summary_service import (
    RunCountsSummary,
    RunSummaryTargetInput,
    build_run_summary,
)

_DEFAULT_RUN_BACKUP_FOR_ROOT = None


@dataclass(frozen=True, slots=True)
class BackupJobRootResult:
    root_id: int
    root_path: str
    status: str
    included_count: int
    skipped_count: int
    manifest_result: ManifestResult | None
    backup_result: BackupServiceResult | None
    error: str | None


@dataclass(frozen=True, slots=True)
class BackupJobLockedResult:
    run: RunLifecycleRecord
    lock_path: str


@dataclass(frozen=True, slots=True)
class BackupJobFinishedResult:
    run: RunLifecycleRecord
    manifest_result: ManifestResult | None
    backup_result: BackupServiceResult | None
    roots: tuple[BackupJobRootResult, ...]
    summary: RunCountsSummary
    report: RunReportArtifacts
    log_file_path: str


@dataclass(slots=True)
class _RootAccumulator:
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

    def to_result(self) -> BackupJobRootResult:
        return BackupJobRootResult(
            root_id=self.root_id,
            root_path=self.root_path,
            status=self.status or "completed",
            included_count=self.included_count,
            skipped_count=self.skipped_count,
            manifest_result=self.manifest_result,
            backup_result=self.backup_result,
            error=self.error,
        )


def run_backup_job(
    *,
    session: Session,
    config: ProjectConfig,
    trigger_mode: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> BackupJobFinishedResult | BackupJobLockedResult:
    run = start_run(
        session=session,
        run_type="backup",
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
        return BackupJobLockedResult(
            run=lock_result.run,
            lock_path=lock_result.lock_path,
        )

    with lock_result:
        try:
            logging_context = configure_run_logging(
                RunLoggingConfig(
                    run_id=run.id,
                    logs_dir=_resolve_runtime_dir(config, config.app_config.runtime.logs_dir),
                    logger_name=f"backup_projects.backup.run.{run.id}",
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
            enabled_raid_names = {
                raid_root.name
                for raid_root in config.app_config.raid_roots
                if raid_root.enabled
            }
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

            manifests_dir = _resolve_runtime_dir(
                config,
                config.app_config.runtime.manifests_dir,
            )
            _validate_existing_dir(manifests_dir, label="manifests_dir")

            logger.info("Backup roots loaded")
            events.append(
                append_run_event(
                    session=session,
                    run_id=run.id,
                    event_type="backup_roots_loaded",
                    message="Backup roots loaded",
                    payload={
                        "enabled_raid_count": len(enabled_raid_names),
                        "active_root_count": len(active_roots),
                    },
                    now=now,
                )
            )

            run_manifest_result: ManifestResult | None = None
            run_backup_result: BackupServiceResult | None = None
            run_timestamp = _format_artifact_timestamp(_resolve_now(now))
            manifest_plan = build_multi_root_dry_run_manifest(
                session=session,
                root_ids=tuple(root.id for root in active_roots),
            )
            active_roots_by_id = {root.id: root for root in active_roots}

            for root_plan in manifest_plan.root_plans:
                target = roots_by_id[root_plan.root_id]
                if root_plan.status == "failed":
                    target.mark_failed(root_plan.error or "Failed to build root manifest")
                    root = active_roots_by_id[root_plan.root_id]
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="backup_root_failed",
                            message=f"Backup failed during manifest planning for root: {root.path}",
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
                root for root in active_roots if roots_by_id[root.id].status != "failed"
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
                    for root in backup_roots:
                        roots_by_id[root.id].mark_failed(str(exc))
                    events.append(
                        append_run_event(
                            session=session,
                            run_id=run.id,
                            event_type="backup_failed",
                            message="Backup failed",
                            payload={"error": str(exc)},
                            level="ERROR",
                            now=now,
                        )
                    )
                    logger.exception("Backup failed")
                else:
                    for root in backup_roots:
                        roots_by_id[root.id].mark_completed()

                    if run_backup_result.restic_result is None:
                        events.append(
                            append_run_event(
                                session=session,
                                run_id=run.id,
                                event_type="backup_skipped",
                                message="Backup skipped",
                                payload={
                                    "manifest_file_path": run_manifest_result.manifest_file_path,
                                    "message": run_backup_result.message,
                                },
                                now=now,
                            )
                        )
                        logger.info("Backup skipped: %s", run_backup_result.message)
                    else:
                        events.append(
                            append_run_event(
                                session=session,
                                run_id=run.id,
                                event_type="backup_completed",
                                message="Backup completed",
                                payload={
                                    "manifest_file_path": run_manifest_result.manifest_file_path,
                                    "snapshot_id": run_backup_result.restic_result.snapshot_id,
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
                    manifest_result=run_manifest_result,
                    backup_result=(
                        run_backup_result.restic_result
                        if run_backup_result is not None
                        and run_backup_result.restic_result is not None
                        else None
                    ),
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
                backup_result=(
                    run_backup_result.restic_result
                    if run_backup_result is not None
                    and run_backup_result.restic_result is not None
                    else None
                ),
            )
            return BackupJobFinishedResult(
                run=finalized_run,
                manifest_result=run_manifest_result,
                backup_result=run_backup_result,
                roots=roots,
                summary=summary,
                report=report,
                log_file_path=str(logging_context.log_file_path),
            )
        except Exception as exc:
            logger.exception("Backup job failed")
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


def _validate_existing_dir(path: Path, *, label: str) -> Path:
    if not path.exists():
        raise ValueError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} is not a directory: {path}")
    return path


def _resolve_now(now: Callable[[], datetime] | None) -> datetime:
    return datetime.now(timezone.utc) if now is None else now()


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _format_artifact_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _build_artifact_stem(*, run_id: int, run_timestamp: str) -> str:
    return f"backup-{run_timestamp}-run-{run_id}"


def _run_backup_for_root(
    *,
    session: Session,
    root: RootRecord,
    config: ProjectConfig,
    manifests_dir: Path,
    run_timestamp: str,
) -> tuple[ManifestResult, BackupServiceResult]:
    built_manifest = build_multi_root_dry_run_manifest(
        session=session,
        root_ids=(root.id,),
    ).built_manifest
    manifest_result = write_manifest(
        built_manifest=built_manifest,
        output_dir=manifests_dir,
        artifact_stem=f"backup-{run_timestamp}-root-{root.id}",
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
    roots: tuple[BackupJobRootResult, ...],
) -> tuple[RunSummaryTargetInput, ...]:
    return tuple(
        RunSummaryTargetInput(
            status=root.status,
            root_id=root.root_id,
            root_path=root.root_path,
            included_count=root.included_count,
            skipped_count=root.skipped_count,
        )
        for root in roots
    )


def _build_report_targets(
    roots: tuple[BackupJobRootResult, ...],
) -> tuple[RunReportTargetInput, ...]:
    return tuple(
        RunReportTargetInput(
            status=root.status,
            root_id=root.root_id,
            root_path=root.root_path,
            included_count=root.included_count,
            skipped_count=root.skipped_count,
            error=root.error,
        )
        for root in roots
    )


def _compute_final_status(roots: tuple[BackupJobRootResult, ...]) -> str:
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
                event_type="backup_job_failed",
                message="Backup job failed",
                payload={"error": error},
                level="ERROR",
                now=now,
            )
        )
    except Exception:
        return
