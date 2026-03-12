from enum import StrEnum

AAF_EXTENSION = "aaf"
AAF_SIZE_LIMIT_BYTES = 100 * 1024 * 1024

DEFAULT_ALLOWED_EXTENSIONS = (
    "prproj",
    "avb",
    "avp",
    "aep",
    "aepx",
    "drp",
    "drt",
    "edl",
    "xml",
    "fcpxml",
    AAF_EXTENSION,
)

DEFAULT_EXCLUDE_DIRECTORY_NAMES = (
    "Cache",
    "Render Cache",
    "Media Cache",
    "Preview Files",
)
DEFAULT_EXCLUDE_GLOB_PATTERNS = ("**/.cache/**",)

DEFAULT_SCHEDULER_MODE = "cron"


class SchedulerMode(StrEnum):
    CRON = DEFAULT_SCHEDULER_MODE


class OversizeAction(StrEnum):
    SKIP = "skip"
    WARN = "warn"
    INCLUDE = "include"


class UnknownExtensionAction(StrEnum):
    COLLECT_AND_SKIP = "collect_and_skip"
    SKIP_SILENT = "skip_silent"
