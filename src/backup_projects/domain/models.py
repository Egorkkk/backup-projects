from __future__ import annotations

from dataclasses import dataclass

from backup_projects.domain.enums import (
    IncludePathType,
    JobType,
    OversizeAction,
    ProjectDirStatus,
    ProjectFileStatus,
    RootStatus,
)


@dataclass(frozen=True, slots=True)
class RootRecord:
    id: int
    raid_name: str
    name: str
    path: str
    status: RootStatus
    needs_structural_rescan: bool
    first_seen_at: str
    last_seen_at: str
    device_id: int | None = None
    inode: int | None = None
    mtime_ns: int | None = None
    ctime_ns: int | None = None


@dataclass(frozen=True, slots=True)
class ProjectDirRecord:
    id: int
    root_id: int
    relative_path: str
    name: str
    dir_type: str
    status: ProjectDirStatus
    first_seen_at: str
    last_seen_at: str


@dataclass(frozen=True, slots=True)
class ProjectFileRecord:
    id: int
    project_dir_id: int
    relative_path: str
    filename: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    status: ProjectFileStatus
    first_seen_at: str
    last_seen_at: str
    inode: int | None = None


@dataclass(frozen=True, slots=True)
class ManualInclude:
    id: int
    path: str
    include_path_type: IncludePathType
    enabled: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class ExtensionRule:
    id: int
    extension: str
    enabled: bool
    oversize_action: OversizeAction
    created_at: str
    updated_at: str
    max_size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class ExcludedPattern:
    id: int
    pattern_type: str
    pattern_value: str
    enabled: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class RunSummary:
    id: int
    job_type: JobType
    status: str
    started_at: str
    trigger_mode: str
    finished_at: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateFile:
    absolute_path: str
    extension: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    inode: int | None = None
    project_dir_id: int | None = None
    project_file_id: int | None = None
    manual_include_id: int | None = None


@dataclass(frozen=True, slots=True)
class FinalDecision:
    candidate: CandidateFile
    include: bool
    reason: str
    oversize_action: OversizeAction | None = None
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class ManifestResult:
    manifest_paths: tuple[str, ...]
    decisions: tuple[FinalDecision, ...]
