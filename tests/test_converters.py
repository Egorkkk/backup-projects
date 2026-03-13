import pytest

from backup_projects.constants import OversizeAction as ConfigOversizeAction
from backup_projects.converters import (
    include_path_type_from_scalar,
    job_type_from_scalar,
    project_dir_status_from_is_missing,
    project_file_status_from_is_missing,
    root_status_from_is_missing,
    to_domain_oversize_action,
    to_excluded_pattern,
    to_extension_rule,
    to_manual_include,
    to_project_dir_record,
    to_project_file_record,
    to_root_record,
    to_run_summary,
)
from backup_projects.domain import (
    IncludePathType,
    JobType,
    OversizeAction,
    ProjectDirStatus,
    ProjectFileStatus,
    RootRecord,
    RootStatus,
    RunSummary,
)
from backup_projects.repositories.manual_includes_repo import ManualIncludeRecord
from backup_projects.repositories.project_dirs_repo import ProjectDirRecord as RepoProjectDirRecord
from backup_projects.repositories.project_files_repo import (
    ProjectFileRecord as RepoProjectFileRecord,
)
from backup_projects.repositories.roots_repo import RootRecord as RepoRootRecord
from backup_projects.repositories.rules_repo import ExcludedPatternRecord, ExtensionRuleRecord
from backup_projects.repositories.runs_repo import RunRecord


def test_converters_are_importable_from_canonical_location() -> None:
    assert callable(to_root_record)
    assert callable(to_project_dir_record)
    assert callable(to_project_file_record)
    assert callable(to_manual_include)
    assert callable(to_extension_rule)
    assert callable(to_excluded_pattern)
    assert callable(to_run_summary)
    assert callable(to_domain_oversize_action)


def test_status_and_scalar_converters_use_domain_contract_types() -> None:
    assert root_status_from_is_missing(False) is RootStatus.ACTIVE
    assert root_status_from_is_missing(True) is RootStatus.MISSING
    assert project_dir_status_from_is_missing(False) is ProjectDirStatus.ACTIVE
    assert project_file_status_from_is_missing(True) is ProjectFileStatus.MISSING
    assert include_path_type_from_scalar("file") is IncludePathType.FILE
    assert job_type_from_scalar("daily") is JobType.DAILY


def test_oversize_action_boundary_converts_legacy_and_scalar_values() -> None:
    assert to_domain_oversize_action(OversizeAction.SKIP) is OversizeAction.SKIP
    assert to_domain_oversize_action(ConfigOversizeAction.WARN) is OversizeAction.WARN
    assert to_domain_oversize_action("include") is OversizeAction.INCLUDE

    with pytest.raises(ValueError):
        to_domain_oversize_action("invalid-action")


def test_record_to_dto_converters_map_repo_local_records_to_domain_models() -> None:
    root_record = to_root_record(
        RepoRootRecord(
            id=1,
            raid_name="raid_a",
            name="show-a",
            path="/mnt/raid_a/projects/show-a",
            device_id=10,
            inode=20,
            mtime_ns=30,
            ctime_ns=40,
            is_missing=False,
            needs_structural_rescan=True,
            first_seen_at="2026-03-13T10:00:00+00:00",
            last_seen_at="2026-03-13T11:00:00+00:00",
        )
    )
    project_dir_record = to_project_dir_record(
        RepoProjectDirRecord(
            id=2,
            root_id=1,
            relative_path="show-a/episode-1",
            name="episode-1",
            dir_type="premiere",
            is_missing=True,
            first_seen_at="2026-03-13T10:10:00+00:00",
            last_seen_at="2026-03-13T11:10:00+00:00",
        )
    )
    project_file_record = to_project_file_record(
        RepoProjectFileRecord(
            id=3,
            project_dir_id=2,
            relative_path="show-a/episode-1/edit.prproj",
            filename="edit.prproj",
            extension="prproj",
            size_bytes=1024,
            mtime_ns=100,
            ctime_ns=90,
            inode=55,
            is_missing=False,
            first_seen_at="2026-03-13T10:20:00+00:00",
            last_seen_at="2026-03-13T11:20:00+00:00",
        )
    )
    manual_include = to_manual_include(
        ManualIncludeRecord(
            id=4,
            path="/mnt/raid_a/projects/show-a/extra.aaf",
            include_type="file",
            enabled=True,
            created_at="2026-03-13T10:30:00+00:00",
            updated_at="2026-03-13T11:30:00+00:00",
        )
    )
    extension_rule = to_extension_rule(
        ExtensionRuleRecord(
            id=5,
            extension="aaf",
            enabled=True,
            max_size_bytes=104857600,
            oversize_action="skip",
            created_at="2026-03-13T10:40:00+00:00",
            updated_at="2026-03-13T11:40:00+00:00",
        )
    )
    excluded_pattern = to_excluded_pattern(
        ExcludedPatternRecord(
            id=6,
            pattern_type="directory_name",
            pattern_value="Cache",
            enabled=False,
            created_at="2026-03-13T10:50:00+00:00",
            updated_at="2026-03-13T11:50:00+00:00",
        )
    )
    run_summary = to_run_summary(
        RunRecord(
            id=7,
            run_type="weekly",
            status="completed",
            started_at="2026-03-13T12:00:00+00:00",
            finished_at="2026-03-13T12:05:00+00:00",
            trigger_mode="cron",
        )
    )

    assert isinstance(root_record, RootRecord)
    assert root_record.status is RootStatus.ACTIVE
    assert root_record.needs_structural_rescan is True
    assert project_dir_record.status is ProjectDirStatus.MISSING
    assert project_file_record.status is ProjectFileStatus.ACTIVE
    assert manual_include.include_path_type is IncludePathType.FILE
    assert extension_rule.oversize_action is OversizeAction.SKIP
    assert excluded_pattern.pattern_type == "directory_name"
    assert isinstance(run_summary, RunSummary)
    assert run_summary.job_type is JobType.WEEKLY
