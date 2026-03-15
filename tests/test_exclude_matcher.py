from __future__ import annotations

import pytest

from backup_projects.constants import UnknownExtensionAction
from backup_projects.domain import ExcludedPattern, OversizeAction
from backup_projects.services.exclude_matcher import build_exclude_matcher
from backup_projects.services.rule_loader import LoadedPolicyConfig, LoadedPolicySettings


def test_directory_name_pattern_matches_parent_directory_segment() -> None:
    pattern = _make_pattern(id=1, pattern_type="directory_name", pattern_value="Cache")
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(excluded_patterns=(pattern,))
    )

    match = matcher.match_path(relative_path="show-a/Cache/file.mov")

    assert match is not None
    assert match.relative_path == "show-a/Cache/file.mov"
    assert match.pattern == pattern


def test_glob_pattern_matches_relative_path() -> None:
    pattern = _make_pattern(id=2, pattern_type="glob", pattern_value="**/*.tmp")
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(excluded_patterns=(pattern,))
    )

    match = matcher.match_path(relative_path="show-a/renders/cache.tmp")

    assert match is not None
    assert match.pattern == pattern


def test_path_substring_pattern_matches_relative_path() -> None:
    pattern = _make_pattern(
        id=3,
        pattern_type="path_substring",
        pattern_value="/DerivedDataCache/",
    )
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(excluded_patterns=(pattern,))
    )

    match = matcher.match_path(
        relative_path="show-a/DerivedDataCache/metadata/index.bin"
    )

    assert match is not None
    assert match.pattern == pattern


def test_regex_pattern_matches_relative_path() -> None:
    pattern = _make_pattern(
        id=4,
        pattern_type="regex",
        pattern_value=r"(^|/)Render Cache(/|$)",
    )
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(excluded_patterns=(pattern,))
    )

    match = matcher.match_path(relative_path="show-a/Render Cache/frame.exr")

    assert match is not None
    assert match.pattern == pattern


def test_first_match_wins_when_multiple_patterns_match() -> None:
    first_pattern = _make_pattern(
        id=5,
        pattern_type="path_substring",
        pattern_value="/cache/",
    )
    second_pattern = _make_pattern(id=6, pattern_type="glob", pattern_value="**/*.mov")
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(
            excluded_patterns=(first_pattern, second_pattern)
        )
    )

    match = matcher.match_path(relative_path="show-a/cache/preview.mov")

    assert match is not None
    assert match.pattern == first_pattern


def test_match_path_returns_none_when_no_pattern_matches() -> None:
    pattern = _make_pattern(id=7, pattern_type="glob", pattern_value="**/*.tmp")
    matcher = build_exclude_matcher(
        policy_config=_make_policy_config(excluded_patterns=(pattern,))
    )

    match = matcher.match_path(relative_path="show-a/edit/project.prproj")

    assert match is None


def test_build_exclude_matcher_rejects_invalid_regex_pattern() -> None:
    invalid_pattern = _make_pattern(id=8, pattern_type="regex", pattern_value="[")

    with pytest.raises(ValueError, match="Invalid regex exclude pattern 8"):
        build_exclude_matcher(
            policy_config=_make_policy_config(excluded_patterns=(invalid_pattern,))
        )


def test_match_path_rejects_empty_relative_path() -> None:
    matcher = build_exclude_matcher(policy_config=_make_policy_config())

    with pytest.raises(ValueError, match="relative_path must not be empty"):
        matcher.match_path(relative_path="")


def _make_policy_config(
    *,
    excluded_patterns: tuple[ExcludedPattern, ...] = (),
) -> LoadedPolicyConfig:
    return LoadedPolicyConfig(
        extension_rules=(),
        excluded_patterns=excluded_patterns,
        settings=LoadedPolicySettings(
            oversize_default_action=OversizeAction.SKIP,
            oversize_log_skipped=True,
            unknown_extensions_action=UnknownExtensionAction.COLLECT_AND_SKIP.value,
            unknown_extensions_store_in_registry=True,
            unknown_extensions_log_warning=True,
        ),
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
