from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain.models import CandidateFile, FinalDecision, ManifestResult
from backup_projects.services.run_service import RunLifecycleRecord
from backup_projects.services.summary_service import (
    RunSummaryTargetInput,
    build_run_summary,
)


def test_build_run_summary_for_single_target_uses_manifest_and_backup_counts() -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                manifest_result=_make_manifest_result(included_count=2, skipped_count=1),
                backup_result=_make_backup_result(files_new=3, files_changed=1),
            ),
        ),
    )

    assert summary.run_id == 42
    assert summary.run_type == "daily"
    assert summary.status == "completed"
    assert summary.included_count == 2
    assert summary.skipped_count == 1
    assert summary.new_count == 3
    assert summary.changed_count == 1
    assert summary.targets_total == 1
    assert summary.targets_succeeded == 1
    assert summary.targets_failed == 0
    assert summary.targets[0].root_id == 1
    assert summary.targets[0].included_count == 2
    assert summary.targets[0].skipped_count == 1
    assert summary.targets[0].new_count == 3
    assert summary.targets[0].changed_count == 1


def test_build_run_summary_for_multiple_targets_aggregates_counts() -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                manifest_result=_make_manifest_result(included_count=2, skipped_count=1),
                backup_result=_make_backup_result(files_new=3, files_changed=1),
            ),
            RunSummaryTargetInput(
                root_id=2,
                root_path="/mnt/raid_b/show-b",
                status="failed",
                manifest_result=_make_manifest_result(included_count=1, skipped_count=2),
                backup_result=_make_backup_result(files_new=4, files_changed=2),
            ),
        ),
    )

    assert summary.included_count == 3
    assert summary.skipped_count == 3
    assert summary.new_count == 7
    assert summary.changed_count == 3
    assert summary.targets_total == 2
    assert summary.targets_succeeded == 1
    assert summary.targets_failed == 1


def test_build_run_summary_keeps_missing_backup_counts_as_none() -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                status="completed",
                manifest_result=_make_manifest_result(included_count=1, skipped_count=0),
                backup_result=_make_backup_result(files_new=None, files_changed=None),
            ),
        ),
    )

    assert summary.new_count is None
    assert summary.changed_count is None
    assert summary.targets[0].new_count is None
    assert summary.targets[0].changed_count is None


def test_build_run_summary_sets_top_level_optional_backup_counts_to_none_when_any_target_is_missing(
) -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                manifest_result=_make_manifest_result(included_count=1, skipped_count=0),
                backup_result=_make_backup_result(files_new=3, files_changed=1),
            ),
            RunSummaryTargetInput(
                root_id=2,
                root_path="/mnt/raid_b/show-b",
                status="completed",
                manifest_result=_make_manifest_result(included_count=2, skipped_count=1),
                backup_result=_make_backup_result(files_new=None, files_changed=None),
            ),
        ),
    )

    assert summary.new_count is None
    assert summary.changed_count is None
    assert summary.targets[0].new_count == 3
    assert summary.targets[0].changed_count == 1
    assert summary.targets[1].new_count is None
    assert summary.targets[1].changed_count is None


def test_build_run_summary_handles_missing_manifest_data_predictably() -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                status="failed",
                backup_result=_make_backup_result(files_new=2, files_changed=1),
            ),
        ),
    )

    assert summary.included_count == 0
    assert summary.skipped_count == 0
    assert summary.new_count == 2
    assert summary.changed_count == 1
    assert summary.targets[0].included_count == 0
    assert summary.targets[0].skipped_count == 0


def test_build_run_summary_excludes_raw_process_and_report_payloads() -> None:
    summary = build_run_summary(
        run=_make_run_record(),
        targets=(
            RunSummaryTargetInput(
                status="completed",
                manifest_result=_make_manifest_result(included_count=1, skipped_count=1),
                backup_result=_make_backup_result(files_new=2, files_changed=1),
            ),
        ),
    )

    assert not hasattr(summary, "summary_payload")
    assert not hasattr(summary, "stdout")
    assert not hasattr(summary, "stderr")
    assert not hasattr(summary, "argv")
    assert not hasattr(summary.targets[0], "summary_payload")
    assert not hasattr(summary.targets[0], "stdout")
    assert not hasattr(summary.targets[0], "stderr")
    assert not hasattr(summary.targets[0], "argv")


def _make_run_record() -> RunLifecycleRecord:
    return RunLifecycleRecord(
        id=42,
        run_type="daily",
        status="completed",
        started_at="2026-03-17T10:00:00+00:00",
        trigger_mode="cron",
        finished_at="2026-03-17T10:05:00+00:00",
    )


def _make_manifest_result(*, included_count: int, skipped_count: int) -> ManifestResult:
    decisions = []
    for index in range(included_count):
        decisions.append(
            FinalDecision(
                candidate=CandidateFile(
                    absolute_path=f"/mnt/raid_a/show-a/included-{index}.prproj",
                    extension="prproj",
                    size_bytes=2048,
                    mtime_ns=100,
                    ctime_ns=90,
                ),
                include=True,
                reason="policy_include",
            )
        )
    for index in range(skipped_count):
        decisions.append(
            FinalDecision(
                candidate=CandidateFile(
                    absolute_path=f"/mnt/raid_a/show-a/skipped-{index}.tmp",
                    extension="tmp",
                    size_bytes=128,
                    mtime_ns=100,
                    ctime_ns=90,
                ),
                include=False,
                reason="excluded",
            )
        )

    return ManifestResult(
        manifest_paths=tuple(
            decision.candidate.absolute_path for decision in decisions if decision.include
        ),
        decisions=tuple(decisions),
        manifest_file_path="/runtime/manifests/run.manifest.txt",
        json_manifest_file_path="/runtime/manifests/run.manifest.json",
        summary_file_path="/runtime/manifests/run.summary.txt",
    )


def _make_backup_result(
    *,
    files_new: int | None,
    files_changed: int | None,
) -> ResticBackupResult:
    summary_payload: dict[str, object] = {"message_type": "summary", "snapshot_id": "snapshot-123"}
    if files_new is not None:
        summary_payload["files_new"] = files_new
    if files_changed is not None:
        summary_payload["files_changed"] = files_changed

    return ResticBackupResult(
        manifest_file_path="/runtime/manifests/run.manifest.txt",
        snapshot_id="snapshot-123",
        summary_payload=summary_payload,
        argv=("restic", "backup"),
        stdout="raw stdout",
        stderr="raw stderr",
        duration_seconds=1.25,
    )
