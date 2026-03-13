from __future__ import annotations

from backup_projects.constants import OversizeAction as ConfigOversizeAction
from backup_projects.domain.enums import (
    IncludePathType,
    JobType,
    ProjectDirStatus,
    ProjectFileStatus,
    RootStatus,
)
from backup_projects.domain.enums import (
    OversizeAction as DomainOversizeAction,
)
from backup_projects.domain.models import (
    ExcludedPattern,
    ExtensionRule,
    ManualInclude,
    RunSummary,
)
from backup_projects.domain.models import (
    ProjectDirRecord as DomainProjectDirRecord,
)
from backup_projects.domain.models import (
    ProjectFileRecord as DomainProjectFileRecord,
)
from backup_projects.domain.models import (
    RootRecord as DomainRootRecord,
)
from backup_projects.repositories.manual_includes_repo import ManualIncludeRecord
from backup_projects.repositories.project_dirs_repo import (
    ProjectDirRecord as RepoProjectDirRecord,
)
from backup_projects.repositories.project_files_repo import (
    ProjectFileRecord as RepoProjectFileRecord,
)
from backup_projects.repositories.roots_repo import RootRecord as RepoRootRecord
from backup_projects.repositories.rules_repo import (
    ExcludedPatternRecord,
    ExtensionRuleRecord,
)
from backup_projects.repositories.runs_repo import RunRecord

__all__ = [
    "include_path_type_from_scalar",
    "job_type_from_scalar",
    "project_dir_status_from_is_missing",
    "project_file_status_from_is_missing",
    "root_status_from_is_missing",
    "to_domain_oversize_action",
    "to_excluded_pattern",
    "to_extension_rule",
    "to_manual_include",
    "to_project_dir_record",
    "to_project_file_record",
    "to_root_record",
    "to_run_summary",
]


def root_status_from_is_missing(is_missing: bool) -> RootStatus:
    return RootStatus.MISSING if is_missing else RootStatus.ACTIVE


def project_dir_status_from_is_missing(is_missing: bool) -> ProjectDirStatus:
    return ProjectDirStatus.MISSING if is_missing else ProjectDirStatus.ACTIVE


def project_file_status_from_is_missing(is_missing: bool) -> ProjectFileStatus:
    return ProjectFileStatus.MISSING if is_missing else ProjectFileStatus.ACTIVE


def include_path_type_from_scalar(value: str | IncludePathType) -> IncludePathType:
    if isinstance(value, IncludePathType):
        return value
    return IncludePathType(value)


def job_type_from_scalar(value: str | JobType) -> JobType:
    if isinstance(value, JobType):
        return value
    return JobType(value)


def to_domain_oversize_action(
    value: str | DomainOversizeAction | ConfigOversizeAction,
) -> DomainOversizeAction:
    if isinstance(value, DomainOversizeAction):
        return value
    if isinstance(value, ConfigOversizeAction):
        return DomainOversizeAction(value.value)
    return DomainOversizeAction(value)


def to_root_record(record: RepoRootRecord) -> DomainRootRecord:
    return DomainRootRecord(
        id=record.id,
        raid_name=record.raid_name,
        name=record.name,
        path=record.path,
        status=root_status_from_is_missing(record.is_missing),
        needs_structural_rescan=record.needs_structural_rescan,
        first_seen_at=record.first_seen_at,
        last_seen_at=record.last_seen_at,
        device_id=record.device_id,
        inode=record.inode,
        mtime_ns=record.mtime_ns,
        ctime_ns=record.ctime_ns,
    )


def to_project_dir_record(record: RepoProjectDirRecord) -> DomainProjectDirRecord:
    return DomainProjectDirRecord(
        id=record.id,
        root_id=record.root_id,
        relative_path=record.relative_path,
        name=record.name,
        dir_type=record.dir_type,
        status=project_dir_status_from_is_missing(record.is_missing),
        first_seen_at=record.first_seen_at,
        last_seen_at=record.last_seen_at,
    )


def to_project_file_record(record: RepoProjectFileRecord) -> DomainProjectFileRecord:
    return DomainProjectFileRecord(
        id=record.id,
        project_dir_id=record.project_dir_id,
        relative_path=record.relative_path,
        filename=record.filename,
        extension=record.extension,
        size_bytes=record.size_bytes,
        mtime_ns=record.mtime_ns,
        ctime_ns=record.ctime_ns,
        status=project_file_status_from_is_missing(record.is_missing),
        first_seen_at=record.first_seen_at,
        last_seen_at=record.last_seen_at,
        inode=record.inode,
    )


def to_manual_include(record: ManualIncludeRecord) -> ManualInclude:
    return ManualInclude(
        id=record.id,
        path=record.path,
        include_path_type=include_path_type_from_scalar(record.include_type),
        enabled=record.enabled,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def to_extension_rule(record: ExtensionRuleRecord) -> ExtensionRule:
    return ExtensionRule(
        id=record.id,
        extension=record.extension,
        enabled=record.enabled,
        oversize_action=to_domain_oversize_action(record.oversize_action),
        created_at=record.created_at,
        updated_at=record.updated_at,
        max_size_bytes=record.max_size_bytes,
    )


def to_excluded_pattern(record: ExcludedPatternRecord) -> ExcludedPattern:
    return ExcludedPattern(
        id=record.id,
        pattern_type=record.pattern_type,
        pattern_value=record.pattern_value,
        enabled=record.enabled,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def to_run_summary(record: RunRecord) -> RunSummary:
    return RunSummary(
        id=record.id,
        job_type=job_type_from_scalar(record.run_type),
        status=record.status,
        started_at=record.started_at,
        trigger_mode=record.trigger_mode,
        finished_at=record.finished_at,
    )
