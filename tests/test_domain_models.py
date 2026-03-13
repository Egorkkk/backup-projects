from dataclasses import FrozenInstanceError, fields, is_dataclass
from typing import get_type_hints

import pytest

from backup_projects.domain import (
    CandidateFile,
    ExcludedPattern,
    ExtensionRule,
    FinalDecision,
    IncludePathType,
    JobType,
    ManifestResult,
    ManualInclude,
    OversizeAction,
    ProjectDirRecord,
    ProjectDirStatus,
    ProjectFileRecord,
    ProjectFileStatus,
    RootRecord,
    RootStatus,
    RunSummary,
)


def test_domain_models_are_importable_from_canonical_package() -> None:
    assert RootRecord.__name__ == "RootRecord"
    assert ProjectDirRecord.__name__ == "ProjectDirRecord"
    assert ProjectFileRecord.__name__ == "ProjectFileRecord"
    assert ManualInclude.__name__ == "ManualInclude"
    assert ExtensionRule.__name__ == "ExtensionRule"
    assert ExcludedPattern.__name__ == "ExcludedPattern"
    assert RunSummary.__name__ == "RunSummary"
    assert CandidateFile.__name__ == "CandidateFile"
    assert FinalDecision.__name__ == "FinalDecision"
    assert ManifestResult.__name__ == "ManifestResult"


@pytest.mark.parametrize(
    "model_cls",
    [
        RootRecord,
        ProjectDirRecord,
        ProjectFileRecord,
        ManualInclude,
        ExtensionRule,
        ExcludedPattern,
        RunSummary,
        CandidateFile,
        FinalDecision,
        ManifestResult,
    ],
)
def test_domain_models_are_frozen_slotted_dataclasses(model_cls: type) -> None:
    assert is_dataclass(model_cls)
    assert model_cls.__dataclass_params__.frozen is True
    assert hasattr(model_cls, "__slots__")


def test_domain_models_use_expected_enum_fields() -> None:
    root_hints = get_type_hints(RootRecord)
    project_dir_hints = get_type_hints(ProjectDirRecord)
    project_file_hints = get_type_hints(ProjectFileRecord)
    manual_include_hints = get_type_hints(ManualInclude)
    extension_rule_hints = get_type_hints(ExtensionRule)
    run_summary_hints = get_type_hints(RunSummary)
    final_decision_hints = get_type_hints(FinalDecision)

    assert root_hints["status"] is RootStatus
    assert project_dir_hints["status"] is ProjectDirStatus
    assert project_file_hints["status"] is ProjectFileStatus
    assert manual_include_hints["include_path_type"] is IncludePathType
    assert extension_rule_hints["oversize_action"] is OversizeAction
    assert run_summary_hints["job_type"] is JobType
    assert final_decision_hints["oversize_action"] == OversizeAction | None


def test_root_record_keeps_status_and_rescan_flag_as_separate_fields() -> None:
    root_field_names = [field.name for field in fields(RootRecord)]

    assert "status" in root_field_names
    assert "needs_structural_rescan" in root_field_names


def test_candidate_file_final_decision_and_manifest_result_stay_minimal() -> None:
    assert [field.name for field in fields(CandidateFile)] == [
        "absolute_path",
        "extension",
        "size_bytes",
        "mtime_ns",
        "ctime_ns",
        "inode",
        "project_dir_id",
        "project_file_id",
        "manual_include_id",
    ]
    assert [field.name for field in fields(FinalDecision)] == [
        "candidate",
        "include",
        "reason",
        "oversize_action",
        "warning",
    ]
    assert [field.name for field in fields(ManifestResult)] == [
        "manifest_paths",
        "decisions",
    ]


def test_domain_models_are_immutable() -> None:
    root = RootRecord(
        id=1,
        raid_name="raid_a",
        name="show-a",
        path="/mnt/raid_a/projects/show-a",
        status=RootStatus.ACTIVE,
        needs_structural_rescan=False,
        first_seen_at="2026-03-13T10:00:00+00:00",
        last_seen_at="2026-03-13T10:00:00+00:00",
    )

    with pytest.raises(FrozenInstanceError):
        root.name = "other-show"
