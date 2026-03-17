from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain.models import ManifestResult
from backup_projects.services.run_service import RunLifecycleRecord


@dataclass(frozen=True, slots=True)
class RunSummaryTargetInput:
    status: str
    root_id: int | None = None
    root_path: str | None = None
    manifest_result: ManifestResult | None = None
    backup_result: ResticBackupResult | None = None


@dataclass(frozen=True, slots=True)
class RunTargetSummary:
    root_id: int | None
    root_path: str | None
    status: str
    included_count: int
    skipped_count: int
    new_count: int | None
    changed_count: int | None


@dataclass(frozen=True, slots=True)
class RunCountsSummary:
    run_id: int
    run_type: str
    status: str
    included_count: int
    skipped_count: int
    new_count: int | None
    changed_count: int | None
    targets_total: int
    targets_succeeded: int
    targets_failed: int
    targets: tuple[RunTargetSummary, ...]


def build_run_summary(
    *,
    run: RunLifecycleRecord,
    targets: Iterable[RunSummaryTargetInput],
) -> RunCountsSummary:
    target_summaries = tuple(_build_target_summary(target) for target in targets)

    return RunCountsSummary(
        run_id=run.id,
        run_type=run.run_type,
        status=run.status,
        included_count=sum(summary.included_count for summary in target_summaries),
        skipped_count=sum(summary.skipped_count for summary in target_summaries),
        new_count=_aggregate_optional_counts(target_summaries, field_name="new_count"),
        changed_count=_aggregate_optional_counts(target_summaries, field_name="changed_count"),
        targets_total=len(target_summaries),
        targets_succeeded=sum(1 for summary in target_summaries if _is_target_success(summary)),
        targets_failed=sum(1 for summary in target_summaries if _is_target_failure(summary)),
        targets=target_summaries,
    )


def _build_target_summary(target: RunSummaryTargetInput) -> RunTargetSummary:
    included_count, skipped_count = _extract_manifest_counts(target.manifest_result)
    new_count = _extract_backup_count(target.backup_result, key="files_new")
    changed_count = _extract_backup_count(target.backup_result, key="files_changed")

    return RunTargetSummary(
        root_id=target.root_id,
        root_path=target.root_path,
        status=target.status,
        included_count=included_count,
        skipped_count=skipped_count,
        new_count=new_count,
        changed_count=changed_count,
    )


def _extract_manifest_counts(manifest_result: ManifestResult | None) -> tuple[int, int]:
    if manifest_result is None:
        return (0, 0)

    included_count = sum(1 for decision in manifest_result.decisions if decision.include)
    skipped_count = sum(1 for decision in manifest_result.decisions if not decision.include)
    return (included_count, skipped_count)


def _extract_backup_count(
    backup_result: ResticBackupResult | None,
    *,
    key: str,
) -> int | None:
    if backup_result is None:
        return None
    value = backup_result.summary_payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _aggregate_optional_counts(
    targets: tuple[RunTargetSummary, ...],
    *,
    field_name: str,
) -> int | None:
    if not targets:
        return None

    values: list[int] = []
    for target in targets:
        value = getattr(target, field_name)
        if value is None:
            return None
        values.append(value)
    return sum(values)


def _is_target_success(target: RunTargetSummary) -> bool:
    return target.status == "completed"


def _is_target_failure(target: RunTargetSummary) -> bool:
    return target.status == "failed"
