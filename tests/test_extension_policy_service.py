from __future__ import annotations

import pytest

from backup_projects.constants import UnknownExtensionAction
from backup_projects.domain import ExtensionRule, OversizeAction
from backup_projects.services.extension_policy_service import (
    build_extension_policy_evaluator,
)
from backup_projects.services.rule_loader import LoadedPolicyConfig, LoadedPolicySettings


def test_known_extension_allowed_returns_expected_reason() -> None:
    rule = _make_extension_rule(id=1, extension="txt")
    evaluator = _build_evaluator(extension_rules=(rule,))

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/notes.txt",
        size_bytes=10,
    )

    assert result.extension == "txt"
    assert result.matched_rule == rule
    assert result.policy_allows_candidate is True
    assert result.reason == "known_extension_allowed"
    assert result.oversize_action is None
    assert result.should_warn is False


def test_unknown_extension_collect_and_skip_sets_warning_and_registry_flags() -> None:
    evaluator = _build_evaluator(
        unknown_extension_action=UnknownExtensionAction.COLLECT_AND_SKIP,
        unknown_extensions_log_warning=True,
    )

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/archive.bin",
        size_bytes=25,
    )

    assert result.extension == "bin"
    assert result.extension_known is False
    assert result.policy_allows_candidate is False
    assert result.reason == "unknown_extension_collect_and_skip"
    assert result.should_warn is True
    assert result.should_store_unknown_extension is True
    assert result.should_log_unknown_extension_warning is True


def test_unknown_extension_skip_silent_sets_no_warning_flags() -> None:
    evaluator = _build_evaluator(
        unknown_extension_action=UnknownExtensionAction.SKIP_SILENT,
        unknown_extensions_log_warning=True,
    )

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/archive.bin",
        size_bytes=25,
    )

    assert result.extension == "bin"
    assert result.extension_known is False
    assert result.policy_allows_candidate is False
    assert result.reason == "unknown_extension_skip_silent"
    assert result.should_warn is False
    assert result.should_store_unknown_extension is False
    assert result.should_log_unknown_extension_warning is False


def test_extension_normalization_matches_uppercase_path_and_rule() -> None:
    rule = _make_extension_rule(id=2, extension=".TXT")
    evaluator = _build_evaluator(extension_rules=(rule,))

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/NOTES.TXT",
        size_bytes=10,
    )

    assert result.extension == "txt"
    assert result.matched_rule == rule
    assert result.reason == "known_extension_allowed"


def test_missing_extension_is_treated_as_unknown_extension() -> None:
    evaluator = _build_evaluator(
        unknown_extension_action=UnknownExtensionAction.COLLECT_AND_SKIP,
        unknown_extensions_log_warning=False,
    )

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/README",
        size_bytes=5,
    )

    assert result.extension == ""
    assert result.extension_known is False
    assert result.reason == "unknown_extension_collect_and_skip"
    assert result.should_warn is False


def test_evaluate_candidate_rejects_negative_size_bytes() -> None:
    evaluator = _build_evaluator()

    with pytest.raises(ValueError, match="size_bytes must be >= 0"):
        evaluator.evaluate_candidate(
            relative_path="show-a/edit/notes.txt",
            size_bytes=-1,
        )


def test_known_extension_with_no_max_size_is_allowed() -> None:
    rule = _make_extension_rule(id=3, extension="mov", max_size_bytes=None)
    evaluator = _build_evaluator(extension_rules=(rule,))

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/plate.mov",
        size_bytes=5_000_000_000,
    )

    assert result.policy_allows_candidate is True
    assert result.reason == "known_extension_allowed"
    assert result.max_size_bytes is None
    assert result.is_oversize is False


def test_size_equal_to_max_size_is_allowed() -> None:
    rule = _make_extension_rule(
        id=4,
        extension="mov",
        max_size_bytes=100,
        oversize_action=OversizeAction.SKIP,
    )
    evaluator = _build_evaluator(extension_rules=(rule,))

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/plate.mov",
        size_bytes=100,
    )

    assert result.policy_allows_candidate is True
    assert result.reason == "known_extension_allowed"
    assert result.is_oversize is False
    assert result.oversize_action is None


@pytest.mark.parametrize(
    ("oversize_action", "allows_candidate", "should_warn", "reason"),
    [
        (
            OversizeAction.SKIP,
            False,
            True,
            "known_extension_oversize_skip",
        ),
        (
            OversizeAction.WARN,
            True,
            True,
            "known_extension_oversize_warn",
        ),
        (
            OversizeAction.INCLUDE,
            True,
            False,
            "known_extension_oversize_include",
        ),
    ],
)
def test_oversize_actions_return_expected_policy_result(
    oversize_action: OversizeAction,
    allows_candidate: bool,
    should_warn: bool,
    reason: str,
) -> None:
    rule = _make_extension_rule(
        id=5,
        extension="mov",
        max_size_bytes=100,
        oversize_action=oversize_action,
    )
    evaluator = _build_evaluator(
        extension_rules=(rule,),
        oversize_log_skipped=True,
    )

    result = evaluator.evaluate_candidate(
        relative_path="show-a/edit/plate.mov",
        size_bytes=101,
    )

    assert result.extension == "mov"
    assert result.matched_rule == rule
    assert result.is_oversize is True
    assert result.max_size_bytes == 100
    assert result.oversize_action == oversize_action
    assert result.policy_allows_candidate is allows_candidate
    assert result.should_warn is should_warn
    assert result.reason == reason


def _build_evaluator(
    *,
    extension_rules: tuple[ExtensionRule, ...] = (),
    unknown_extension_action: UnknownExtensionAction = (
        UnknownExtensionAction.COLLECT_AND_SKIP
    ),
    unknown_extensions_log_warning: bool = True,
    oversize_log_skipped: bool = True,
) -> object:
    return build_extension_policy_evaluator(
        policy_config=LoadedPolicyConfig(
            extension_rules=extension_rules,
            excluded_patterns=(),
            settings=LoadedPolicySettings(
                oversize_default_action=OversizeAction.SKIP,
                oversize_log_skipped=oversize_log_skipped,
                unknown_extensions_action=unknown_extension_action.value,
                unknown_extensions_store_in_registry=True,
                unknown_extensions_log_warning=unknown_extensions_log_warning,
            ),
        )
    )


def _make_extension_rule(
    *,
    id: int,
    extension: str,
    max_size_bytes: int | None = None,
    oversize_action: OversizeAction = OversizeAction.SKIP,
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
