from __future__ import annotations

from enum import StrEnum


class RootStatus(StrEnum):
    ACTIVE = "active"
    MISSING = "missing"


class ProjectDirStatus(StrEnum):
    ACTIVE = "active"
    MISSING = "missing"


class ProjectFileStatus(StrEnum):
    ACTIVE = "active"
    MISSING = "missing"


class JobType(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"


class OversizeAction(StrEnum):
    SKIP = "skip"
    WARN = "warn"
    INCLUDE = "include"


class IncludePathType(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
