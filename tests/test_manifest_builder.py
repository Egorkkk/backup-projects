import json
from pathlib import Path

import pytest

from backup_projects.domain import (
    CandidateFile,
    FinalDecision,
    ManifestResult,
    OversizeAction,
)
from backup_projects.services.manifest_builder import (
    BuiltManifest,
    build_manifest,
    write_manifest,
)


def test_build_manifest_keeps_all_decisions_and_builds_deterministic_artifacts() -> None:
    included_warn = _make_decision(
        absolute_path="/mnt/raid_a/projects/show-a/b.mov",
        include=True,
        reason="policy_oversize_warn_include",
        size_bytes=20,
        warning="oversize",
        manual_include_applied=True,
        oversize_action=OversizeAction.WARN,
        extension_policy_reason="known_extension_oversize_warn",
    )
    skipped = _make_decision(
        absolute_path="/mnt/raid_a/projects/show-a/a.bin",
        include=False,
        reason="policy_unknown_extension_skip",
        size_bytes=30,
        warning="unknown_extension",
        should_store_unknown_extension=True,
        should_log_unknown_extension_warning=True,
        extension_policy_reason="unknown_extension_collect_and_skip",
    )
    included_plain = _make_decision(
        absolute_path="/mnt/raid_a/projects/show-a/c.txt",
        include=True,
        reason="policy_include",
        size_bytes=10,
        force_include_applied=True,
        extension_policy_reason="known_extension_allowed",
    )

    built_manifest = build_manifest(
        decisions=[included_plain, skipped, included_warn]
    )

    assert built_manifest.manifest_paths == (
        "/mnt/raid_a/projects/show-a/b.mov",
        "/mnt/raid_a/projects/show-a/c.txt",
    )
    assert tuple(
        decision.candidate.absolute_path for decision in built_manifest.decisions
    ) == (
        "/mnt/raid_a/projects/show-a/a.bin",
        "/mnt/raid_a/projects/show-a/b.mov",
        "/mnt/raid_a/projects/show-a/c.txt",
    )
    assert built_manifest.json_payload == {
        "format_version": 1,
        "counts": {
            "total_decisions": 3,
            "included": 2,
            "skipped": 1,
            "warnings": 2,
            "included_bytes": 30,
        },
        "included_files": [
            {
                "path": "/mnt/raid_a/projects/show-a/b.mov",
                "size_bytes": 20,
                "reason": "policy_oversize_warn_include",
                "warning": "oversize",
                "manual_include_applied": True,
                "force_include_applied": False,
                "oversize_action": "warn",
                "extension_policy_reason": "known_extension_oversize_warn",
            },
            {
                "path": "/mnt/raid_a/projects/show-a/c.txt",
                "size_bytes": 10,
                "reason": "policy_include",
                "warning": None,
                "manual_include_applied": False,
                "force_include_applied": True,
                "oversize_action": None,
                "extension_policy_reason": "known_extension_allowed",
            },
        ],
        "skipped_counts_by_reason": {
            "policy_unknown_extension_skip": 1,
        },
        "warning_counts_by_type": {
            "oversize": 1,
            "unknown_extension": 1,
        },
    }
    assert built_manifest.summary_text == "\n".join(
        [
            "Manifest summary",
            "Total decisions: 3",
            "Included: 2",
            "Skipped: 1",
            "Warnings: 2",
            "Included bytes: 30",
            "",
            "Reason counts:",
            "- policy_include: 1",
            "- policy_oversize_warn_include: 1",
            "- policy_unknown_extension_skip: 1",
            "",
            "Warning counts:",
            "- oversize: 1",
            "- unknown_extension: 1",
        ]
    )


def test_build_manifest_allows_empty_input() -> None:
    built_manifest = build_manifest(decisions=[])

    assert built_manifest == BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={
            "format_version": 1,
            "counts": {
                "total_decisions": 0,
                "included": 0,
                "skipped": 0,
                "warnings": 0,
                "included_bytes": 0,
            },
            "included_files": [],
            "skipped_counts_by_reason": {},
            "warning_counts_by_type": {},
        },
        summary_text="\n".join(
            [
                "Manifest summary",
                "Total decisions: 0",
                "Included: 0",
                "Skipped: 0",
                "Warnings: 0",
                "Included bytes: 0",
                "",
                "Reason counts:",
                "- none: 0",
                "",
                "Warning counts:",
                "- none: 0",
            ]
        ),
    )


def test_build_manifest_rejects_duplicate_absolute_paths() -> None:
    duplicate_a = _make_decision(
        absolute_path="/mnt/raid_a/projects/show-a/shared.mov",
        include=True,
        reason="policy_include",
    )
    duplicate_b = _make_decision(
        absolute_path="/mnt/raid_a/projects/show-a/shared.mov",
        include=False,
        reason="excluded",
    )

    with pytest.raises(
        ValueError,
        match="Duplicate candidate.absolute_path detected",
    ):
        build_manifest(decisions=[duplicate_a, duplicate_b])


def test_write_manifest_persists_three_artifacts_and_returns_manifest_result(
    tmp_path: Path,
) -> None:
    built_manifest = build_manifest(
        decisions=[
            _make_decision(
                absolute_path="/mnt/raid_a/projects/show-a/file-b.txt",
                include=True,
                reason="policy_include",
                size_bytes=42,
                extension_policy_reason="known_extension_allowed",
            ),
            _make_decision(
                absolute_path="/mnt/raid_a/projects/show-a/file-a.txt",
                include=True,
                reason="policy_include",
                size_bytes=10,
                extension_policy_reason="known_extension_allowed",
            ),
        ]
    )

    result = write_manifest(
        built_manifest=built_manifest,
        output_dir=tmp_path,
        artifact_stem="daily-run",
    )

    assert result == ManifestResult(
        manifest_paths=(
            "/mnt/raid_a/projects/show-a/file-a.txt",
            "/mnt/raid_a/projects/show-a/file-b.txt",
        ),
        decisions=built_manifest.decisions,
        manifest_file_path=str(tmp_path / "daily-run.manifest.txt"),
        json_manifest_file_path=str(tmp_path / "daily-run.manifest.json"),
        summary_file_path=str(tmp_path / "daily-run.summary.txt"),
    )
    assert (tmp_path / "daily-run.manifest.txt").read_text(encoding="utf-8") == (
        "/mnt/raid_a/projects/show-a/file-a.txt\n"
        "/mnt/raid_a/projects/show-a/file-b.txt\n"
    )
    assert json.loads(
        (tmp_path / "daily-run.manifest.json").read_text(encoding="utf-8")
    ) == built_manifest.json_payload
    assert (tmp_path / "daily-run.summary.txt").read_text(encoding="utf-8") == (
        built_manifest.summary_text + "\n"
    )
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "daily-run.manifest.json",
        "daily-run.manifest.txt",
        "daily-run.summary.txt",
    ]

    file_path = tmp_path / "not-a-dir.txt"
    file_path.write_text("x", encoding="utf-8")

    with pytest.raises(ValueError, match="output_dir does not exist"):
        write_manifest(
            built_manifest=built_manifest,
            output_dir=tmp_path / "missing",
            artifact_stem="daily-run",
        )
    with pytest.raises(ValueError, match="output_dir is not a directory"):
        write_manifest(
            built_manifest=built_manifest,
            output_dir=file_path,
            artifact_stem="daily-run",
        )
    with pytest.raises(ValueError, match="artifact_stem must not be empty"):
        write_manifest(
            built_manifest=built_manifest,
            output_dir=tmp_path,
            artifact_stem="   ",
        )


def test_write_manifest_writes_empty_artifacts_for_empty_built_manifest(
    tmp_path: Path,
) -> None:
    built_manifest = build_manifest(decisions=[])

    result = write_manifest(
        built_manifest=built_manifest,
        output_dir=tmp_path,
        artifact_stem="empty-run",
    )

    assert result.manifest_paths == ()
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "empty-run.manifest.json",
        "empty-run.manifest.txt",
        "empty-run.summary.txt",
    ]
    assert (tmp_path / "empty-run.manifest.txt").read_text(encoding="utf-8") == ""
    assert json.loads(
        (tmp_path / "empty-run.manifest.json").read_text(encoding="utf-8")
    ) == built_manifest.json_payload
    assert (tmp_path / "empty-run.summary.txt").read_text(encoding="utf-8") == (
        built_manifest.summary_text + "\n"
    )


def _make_decision(
    *,
    absolute_path: str,
    include: bool,
    reason: str,
    size_bytes: int = 1,
    warning: str | None = None,
    manual_include_applied: bool = False,
    force_include_applied: bool = False,
    oversize_action: OversizeAction | None = None,
    extension_policy_reason: str | None = None,
    should_store_unknown_extension: bool = False,
    should_log_unknown_extension_warning: bool = False,
) -> FinalDecision:
    return FinalDecision(
        candidate=CandidateFile(
            absolute_path=absolute_path,
            extension=Path(absolute_path).suffix.removeprefix("."),
            size_bytes=size_bytes,
            mtime_ns=1,
            ctime_ns=2,
        ),
        include=include,
        reason=reason,
        oversize_action=oversize_action,
        warning=warning,
        manual_include_applied=manual_include_applied,
        force_include_applied=force_include_applied,
        extension_policy_reason=extension_policy_reason,
        should_store_unknown_extension=should_store_unknown_extension,
        should_log_unknown_extension_warning=should_log_unknown_extension_warning,
    )
