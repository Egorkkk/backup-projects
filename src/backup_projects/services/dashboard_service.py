from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ProjectConfig
from backup_projects.services.run_service import RunLifecycleRecord
from backup_projects.services.run_visibility_service import get_run_details, list_runs


@dataclass(frozen=True, slots=True)
class DashboardRunItem:
    run_id: int | None
    run_type: str | None
    status: str | None
    trigger_mode: str | None
    started_at: str | None
    finished_at: str | None


@dataclass(frozen=True, slots=True)
class DashboardCounts:
    included: int | None
    skipped: int | None
    new: int | None
    changed: int | None


@dataclass(frozen=True, slots=True)
class DashboardOversizedSummary:
    skipped_count: int | None
    warning_count: int | None


@dataclass(frozen=True, slots=True)
class DashboardView:
    last_scan: DashboardRunItem
    last_backup: DashboardRunItem
    run_status: DashboardRunItem
    counts: DashboardCounts
    skipped_oversized_summary: DashboardOversizedSummary


def build_dashboard_view(
    *,
    session: Session,
    config: ProjectConfig,
) -> DashboardView:
    runs = list_runs(session=session, limit=100)

    latest_run = runs[0] if runs else None
    last_scan = _find_latest_run(runs, run_types={"scan", "daily"})
    last_backup = _find_latest_run(runs, run_types={"backup", "daily"})
    counts = DashboardCounts(
        included=None,
        skipped=None,
        new=None,
        changed=None,
    )
    oversized_summary = DashboardOversizedSummary(
        skipped_count=None,
        warning_count=None,
    )

    if last_backup is not None:
        counts, oversized_summary = _load_backup_run_metrics(
            session=session,
            config=config,
            run=last_backup,
        )

    return DashboardView(
        last_scan=_to_dashboard_run_item(last_scan),
        last_backup=_to_dashboard_run_item(last_backup),
        run_status=_to_dashboard_run_item(latest_run),
        counts=counts,
        skipped_oversized_summary=oversized_summary,
    )


def _find_latest_run(
    runs: tuple[RunLifecycleRecord, ...],
    *,
    run_types: set[str],
) -> RunLifecycleRecord | None:
    for run in runs:
        if run.run_type in run_types:
            return run
    return None


def _to_dashboard_run_item(run: RunLifecycleRecord | None) -> DashboardRunItem:
    if run is None:
        return DashboardRunItem(
            run_id=None,
            run_type=None,
            status=None,
            trigger_mode=None,
            started_at=None,
            finished_at=None,
        )

    return DashboardRunItem(
        run_id=run.id,
        run_type=run.run_type,
        status=run.status,
        trigger_mode=run.trigger_mode,
        started_at=run.started_at,
        finished_at=run.finished_at,
    )


def _load_backup_run_metrics(
    *,
    session: Session,
    config: ProjectConfig,
    run: RunLifecycleRecord,
) -> tuple[DashboardCounts, DashboardOversizedSummary]:
    run_details = get_run_details(
        session=session,
        run_id=run.id,
        reports_dir=_resolve_runtime_dir(config, config.app_config.runtime.reports_dir),
        logs_dir=_resolve_runtime_dir(config, config.app_config.runtime.logs_dir),
    )
    if not run_details.report_json.exists:
        return (
            DashboardCounts(included=None, skipped=None, new=None, changed=None),
            DashboardOversizedSummary(skipped_count=None, warning_count=None),
        )

    report_payload = _load_json_file(Path(run_details.report_json.path))
    if not isinstance(report_payload, dict):
        return (
            DashboardCounts(included=None, skipped=None, new=None, changed=None),
            DashboardOversizedSummary(skipped_count=None, warning_count=None),
        )

    targets = report_payload.get("targets")
    if not isinstance(targets, list):
        return (
            DashboardCounts(included=None, skipped=None, new=None, changed=None),
            DashboardOversizedSummary(skipped_count=None, warning_count=None),
        )

    report_manifest_payload = _load_manifest_payload(report_payload.get("manifest"))
    included_total = 0
    skipped_total = 0
    oversize_skipped_total = 0
    oversize_warning_total = 0
    new_total = 0
    changed_total = 0
    saw_manifest_counts = False
    saw_backup_counts = False

    for target in targets:
        if not isinstance(target, dict):
            continue

        manifest_payload = _load_target_manifest_payload(target)
        if manifest_payload is not None:
            counts_payload = manifest_payload.get("counts")
            if isinstance(counts_payload, dict):
                included_value = counts_payload.get("included")
                skipped_value = counts_payload.get("skipped")
                if isinstance(included_value, int) and not isinstance(included_value, bool):
                    included_total += included_value
                    saw_manifest_counts = True
                if isinstance(skipped_value, int) and not isinstance(skipped_value, bool):
                    skipped_total += skipped_value
                    saw_manifest_counts = True

            skipped_by_reason = manifest_payload.get("skipped_counts_by_reason")
            if isinstance(skipped_by_reason, dict):
                oversize_skipped_value = skipped_by_reason.get("policy_oversize_skip")
                if isinstance(oversize_skipped_value, int) and not isinstance(
                    oversize_skipped_value,
                    bool,
                ):
                    oversize_skipped_total += oversize_skipped_value
                    saw_manifest_counts = True

            warnings_by_type = manifest_payload.get("warning_counts_by_type")
            if isinstance(warnings_by_type, dict):
                oversize_warning_value = warnings_by_type.get("oversize")
                if isinstance(oversize_warning_value, int) and not isinstance(
                    oversize_warning_value,
                    bool,
                ):
                    oversize_warning_total += oversize_warning_value
                    saw_manifest_counts = True
        else:
            included_value = target.get("included_count")
            skipped_value = target.get("skipped_count")
            if isinstance(included_value, int) and not isinstance(included_value, bool):
                included_total += included_value
                saw_manifest_counts = True
            if isinstance(skipped_value, int) and not isinstance(skipped_value, bool):
                skipped_total += skipped_value
                saw_manifest_counts = True

        backup_payload = target.get("backup")
        if isinstance(backup_payload, dict):
            summary_payload = backup_payload.get("summary_payload")
            if isinstance(summary_payload, dict):
                new_value = summary_payload.get("files_new")
                changed_value = summary_payload.get("files_changed")
                if isinstance(new_value, int) and not isinstance(new_value, bool):
                    new_total += new_value
                    saw_backup_counts = True
                if isinstance(changed_value, int) and not isinstance(changed_value, bool):
                    changed_total += changed_value
                    saw_backup_counts = True

    if not saw_manifest_counts and report_manifest_payload is not None:
        counts_payload = report_manifest_payload.get("counts")
        if isinstance(counts_payload, dict):
            included_value = counts_payload.get("included")
            skipped_value = counts_payload.get("skipped")
            if isinstance(included_value, int) and not isinstance(included_value, bool):
                included_total = included_value
                saw_manifest_counts = True
            if isinstance(skipped_value, int) and not isinstance(skipped_value, bool):
                skipped_total = skipped_value
                saw_manifest_counts = True

        skipped_by_reason = report_manifest_payload.get("skipped_counts_by_reason")
        if isinstance(skipped_by_reason, dict):
            oversize_skipped_value = skipped_by_reason.get("policy_oversize_skip")
            if isinstance(oversize_skipped_value, int) and not isinstance(
                oversize_skipped_value,
                bool,
            ):
                oversize_skipped_total = oversize_skipped_value
                saw_manifest_counts = True

        warnings_by_type = report_manifest_payload.get("warning_counts_by_type")
        if isinstance(warnings_by_type, dict):
            oversize_warning_value = warnings_by_type.get("oversize")
            if isinstance(oversize_warning_value, int) and not isinstance(
                oversize_warning_value,
                bool,
            ):
                oversize_warning_total = oversize_warning_value
                saw_manifest_counts = True

    if not saw_backup_counts:
        report_backup = report_payload.get("backup")
        if isinstance(report_backup, dict):
            summary_payload = report_backup.get("summary_payload")
            if isinstance(summary_payload, dict):
                new_value = summary_payload.get("files_new")
                changed_value = summary_payload.get("files_changed")
                if isinstance(new_value, int) and not isinstance(new_value, bool):
                    new_total = new_value
                    saw_backup_counts = True
                if isinstance(changed_value, int) and not isinstance(changed_value, bool):
                    changed_total = changed_value
                    saw_backup_counts = True

    return (
        DashboardCounts(
            included=included_total if saw_manifest_counts else None,
            skipped=skipped_total if saw_manifest_counts else None,
            new=new_total if saw_backup_counts else None,
            changed=changed_total if saw_backup_counts else None,
        ),
        DashboardOversizedSummary(
            skipped_count=oversize_skipped_total if saw_manifest_counts else None,
            warning_count=oversize_warning_total if saw_manifest_counts else None,
        ),
    )


def _load_target_manifest_payload(target: dict[str, object]) -> dict[str, object] | None:
    return _load_manifest_payload(target.get("manifest"))


def _load_manifest_payload(manifest_payload: object) -> dict[str, object] | None:
    if not isinstance(manifest_payload, dict):
        return None

    json_manifest_file_path = manifest_payload.get("json_manifest_file_path")
    if not isinstance(json_manifest_file_path, str) or json_manifest_file_path.strip() == "":
        return None

    loaded_payload = _load_json_file(Path(json_manifest_file_path))
    if not isinstance(loaded_payload, dict):
        return None
    return loaded_payload


def _load_json_file(path: Path) -> object | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, IsADirectoryError, OSError, json.JSONDecodeError):
        return None


def _resolve_runtime_dir(config: ProjectConfig, runtime_path: str) -> Path:
    return resolve_path(config.app_path.parent / runtime_path)
