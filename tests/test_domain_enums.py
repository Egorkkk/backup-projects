from backup_projects.domain import (
    IncludePathType,
    JobType,
    OversizeAction,
    ProjectDirStatus,
    ProjectFileStatus,
    RootStatus,
)


def test_domain_enums_are_importable_from_canonical_package() -> None:
    assert RootStatus.ACTIVE == "active"
    assert ProjectDirStatus.ACTIVE == "active"
    assert ProjectFileStatus.ACTIVE == "active"
    assert JobType.DAILY == "daily"
    assert OversizeAction.WARN == "warn"
    assert IncludePathType.FILE == "file"


def test_domain_enum_values_are_stable() -> None:
    assert list(RootStatus) == [RootStatus.ACTIVE, RootStatus.MISSING]
    assert list(ProjectDirStatus) == [ProjectDirStatus.ACTIVE, ProjectDirStatus.MISSING]
    assert list(ProjectFileStatus) == [ProjectFileStatus.ACTIVE, ProjectFileStatus.MISSING]
    assert list(JobType) == [JobType.DAILY, JobType.WEEKLY]
    assert list(OversizeAction) == [
        OversizeAction.SKIP,
        OversizeAction.WARN,
        OversizeAction.INCLUDE,
    ]
    assert list(IncludePathType) == [IncludePathType.FILE, IncludePathType.DIRECTORY]


def test_root_status_does_not_encode_rescan_flag_semantics() -> None:
    root_status_names = {member.name for member in RootStatus}
    root_status_values = {member.value for member in RootStatus}

    assert root_status_names == {"ACTIVE", "MISSING"}
    assert root_status_values == {"active", "missing"}
    assert all("rescan" not in value for value in root_status_values)
