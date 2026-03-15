from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from backup_projects.constants import UnknownExtensionAction
from backup_projects.domain import CandidateFile, ExcludedPattern, ExtensionRule
from backup_projects.domain import OversizeAction
from backup_projects.services.decision_engine import (
    DecisionCandidate,
    build_decision_engine,
)
from backup_projects.services.rule_loader import LoadedPolicyConfig, LoadedPolicySettings


def test_force_include_overrides_exclude_and_preserves_policy_signals() -> None:
    exclude_pattern = _make_pattern(
        id=1,
        pattern_type="path_substring",
        pattern_value="/cache/",
    )
    engine = _build_engine(excluded_patterns=(exclude_pattern,))

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/cache/archive.bin",
            size_bytes=20,
            manual_include_id=10,
            force_include=True,
        )
    )

    assert decision.include is True
    assert decision.reason == "force_include_override_exclude"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is True
    assert decision.exclude_pattern == exclude_pattern
    assert decision.extension_policy_reason == "unknown_extension_collect_and_skip"
    assert decision.oversize_action is None
    assert decision.warning == "unknown_extension"
    assert decision.should_store_unknown_extension is True
    assert decision.should_log_unknown_extension_warning is True


def test_force_include_overrides_unknown_extension_skip() -> None:
    engine = _build_engine()

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/archive.bin",
            size_bytes=20,
            manual_include_id=11,
            force_include=True,
        )
    )

    assert decision.include is True
    assert decision.reason == "force_include_override_policy_unknown_extension"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is True
    assert decision.exclude_pattern is None
    assert decision.extension_policy_reason == "unknown_extension_collect_and_skip"
    assert decision.warning == "unknown_extension"
    assert decision.should_store_unknown_extension is True
    assert decision.should_log_unknown_extension_warning is True


def test_force_include_overrides_oversize_skip() -> None:
    engine = _build_engine(
        extension_rules=(
            _make_extension_rule(
                id=2,
                extension="mov",
                max_size_bytes=100,
                oversize_action=OversizeAction.SKIP,
            ),
        )
    )

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/plate.mov",
            size_bytes=101,
            manual_include_id=12,
            force_include=True,
        )
    )

    assert decision.include is True
    assert decision.reason == "force_include_override_policy_oversize"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is True
    assert decision.extension_policy_reason == "known_extension_oversize_skip"
    assert decision.oversize_action is OversizeAction.SKIP
    assert decision.warning == "oversize"


def test_non_forced_manual_include_does_not_override_exclude() -> None:
    exclude_pattern = _make_pattern(
        id=3,
        pattern_type="path_substring",
        pattern_value="/cache/",
    )
    engine = _build_engine(excluded_patterns=(exclude_pattern,))

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/cache/archive.bin",
            size_bytes=20,
            manual_include_id=20,
        )
    )

    assert decision.include is False
    assert decision.reason == "excluded"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is False
    assert decision.exclude_pattern == exclude_pattern
    assert decision.warning == "unknown_extension"


def test_non_forced_manual_include_does_not_override_unknown_extension_skip() -> None:
    engine = _build_engine()

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/archive.bin",
            size_bytes=20,
            manual_include_id=21,
        )
    )

    assert decision.include is False
    assert decision.reason == "policy_unknown_extension_skip"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is False
    assert decision.extension_policy_reason == "unknown_extension_collect_and_skip"
    assert decision.warning == "unknown_extension"
    assert decision.should_store_unknown_extension is True
    assert decision.should_log_unknown_extension_warning is True


def test_non_forced_manual_include_does_not_override_oversize_skip() -> None:
    engine = _build_engine(
        extension_rules=(
            _make_extension_rule(
                id=4,
                extension="mov",
                max_size_bytes=100,
                oversize_action=OversizeAction.SKIP,
            ),
        )
    )

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/plate.mov",
            size_bytes=101,
            manual_include_id=22,
        )
    )

    assert decision.include is False
    assert decision.reason == "policy_oversize_skip"
    assert decision.manual_include_applied is True
    assert decision.force_include_applied is False
    assert decision.extension_policy_reason == "known_extension_oversize_skip"
    assert decision.oversize_action is OversizeAction.SKIP
    assert decision.warning == "oversize"


def test_oversize_warn_preserves_warning_and_action() -> None:
    engine = _build_engine(
        extension_rules=(
            _make_extension_rule(
                id=5,
                extension="mov",
                max_size_bytes=100,
                oversize_action=OversizeAction.WARN,
            ),
        )
    )

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/plate.mov",
            size_bytes=101,
        )
    )

    assert decision.include is True
    assert decision.reason == "policy_oversize_warn_include"
    assert decision.oversize_action is OversizeAction.WARN
    assert decision.warning == "oversize"
    assert decision.extension_policy_reason == "known_extension_oversize_warn"


def test_oversize_include_preserves_include_decision_and_action() -> None:
    engine = _build_engine(
        extension_rules=(
            _make_extension_rule(
                id=6,
                extension="mov",
                max_size_bytes=100,
                oversize_action=OversizeAction.INCLUDE,
            ),
        )
    )

    decision = engine.evaluate_candidate(
        candidate=_make_decision_candidate(
            relative_path="show-a/edit/plate.mov",
            size_bytes=101,
        )
    )

    assert decision.include is True
    assert decision.reason == "policy_oversize_include"
    assert decision.oversize_action is OversizeAction.INCLUDE
    assert decision.warning is None
    assert decision.extension_policy_reason == "known_extension_oversize_include"


def test_force_include_requires_manual_include_id() -> None:
    engine = _build_engine()

    with pytest.raises(
        ValueError,
        match="DecisionCandidate.force_include requires candidate.manual_include_id",
    ):
        engine.evaluate_candidate(
            candidate=_make_decision_candidate(
                relative_path="show-a/edit/archive.bin",
                size_bytes=1,
                force_include=True,
            )
        )


def test_empty_relative_path_is_rejected() -> None:
    engine = _build_engine()

    with pytest.raises(
        ValueError,
        match="DecisionCandidate.relative_path must not be empty",
    ):
        engine.evaluate_candidate(
            candidate=_make_decision_candidate(relative_path="", size_bytes=1)
        )


def _build_engine(
    *,
    extension_rules: tuple[ExtensionRule, ...] = (),
    excluded_patterns: tuple[ExcludedPattern, ...] = (),
) -> object:
    return build_decision_engine(
        policy_config=LoadedPolicyConfig(
            extension_rules=extension_rules,
            excluded_patterns=excluded_patterns,
            settings=LoadedPolicySettings(
                oversize_default_action=OversizeAction.SKIP,
                oversize_log_skipped=True,
                unknown_extensions_action=UnknownExtensionAction.COLLECT_AND_SKIP.value,
                unknown_extensions_store_in_registry=True,
                unknown_extensions_log_warning=True,
            ),
        )
    )


def _make_decision_candidate(
    *,
    relative_path: str,
    size_bytes: int,
    manual_include_id: int | None = None,
    force_include: bool = False,
) -> DecisionCandidate:
    extension = PurePosixPath(relative_path).suffix.removeprefix(".").lower()
    absolute_path = (
        f"/mnt/raid_a/projects/{relative_path}" if relative_path else "/mnt/raid_a/projects"
    )
    return DecisionCandidate(
        candidate=CandidateFile(
            absolute_path=absolute_path,
            extension=extension,
            size_bytes=size_bytes,
            mtime_ns=1,
            ctime_ns=1,
            manual_include_id=manual_include_id,
        ),
        relative_path=relative_path,
        force_include=force_include,
    )


def _make_extension_rule(
    *,
    id: int,
    extension: str,
    max_size_bytes: int,
    oversize_action: OversizeAction,
) -> ExtensionRule:
    return ExtensionRule(
        id=id,
        extension=extension,
        enabled=True,
        max_size_bytes=max_size_bytes,
        oversize_action=oversize_action,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )


def _make_pattern(
    *,
    id: int,
    pattern_type: str,
    pattern_value: str,
) -> ExcludedPattern:
    return ExcludedPattern(
        id=id,
        pattern_type=pattern_type,
        pattern_value=pattern_value,
        enabled=True,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )
