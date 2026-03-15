from __future__ import annotations

import re
from dataclasses import dataclass
from fnmatch import fnmatchcase

from backup_projects.domain.models import ExcludedPattern
from backup_projects.services.rule_loader import LoadedPolicyConfig

_SUPPORTED_PATTERN_TYPES = frozenset(
    ("directory_name", "glob", "path_substring", "regex")
)


@dataclass(frozen=True, slots=True)
class ExcludeMatch:
    relative_path: str
    pattern: ExcludedPattern


@dataclass(frozen=True, slots=True)
class _CompiledExcludedPattern:
    pattern: ExcludedPattern
    compiled_regex: re.Pattern[str] | None = None


@dataclass(frozen=True, slots=True)
class ExcludeMatcher:
    _compiled_patterns: tuple[_CompiledExcludedPattern, ...]

    def match_path(self, *, relative_path: str) -> ExcludeMatch | None:
        normalized_relative_path = _validate_relative_path(relative_path)

        for compiled_pattern in self._compiled_patterns:
            if _matches_pattern(
                compiled_pattern=compiled_pattern,
                relative_path=normalized_relative_path,
            ):
                return ExcludeMatch(
                    relative_path=normalized_relative_path,
                    pattern=compiled_pattern.pattern,
                )

        return None


def build_exclude_matcher(*, policy_config: LoadedPolicyConfig) -> ExcludeMatcher:
    return ExcludeMatcher(
        _compiled_patterns=tuple(
            _compile_pattern(pattern)
            for pattern in policy_config.excluded_patterns
        )
    )


def _compile_pattern(pattern: ExcludedPattern) -> _CompiledExcludedPattern:
    if pattern.pattern_type not in _SUPPORTED_PATTERN_TYPES:
        raise ValueError(
            f"Unsupported excluded pattern type for pattern {pattern.id}: {pattern.pattern_type}"
        )
    if pattern.pattern_value.strip() == "":
        raise ValueError(f"Excluded pattern {pattern.id} has an empty pattern_value")

    if pattern.pattern_type != "regex":
        return _CompiledExcludedPattern(pattern=pattern)

    try:
        compiled_regex = re.compile(pattern.pattern_value)
    except re.error as exc:
        raise ValueError(
            f"Invalid regex exclude pattern {pattern.id}: {pattern.pattern_value!r}: {exc}"
        ) from exc

    return _CompiledExcludedPattern(pattern=pattern, compiled_regex=compiled_regex)


def _validate_relative_path(relative_path: str) -> str:
    if relative_path == "":
        raise ValueError("relative_path must not be empty")
    if relative_path == ".":
        raise ValueError("relative_path must not be '.'")
    if relative_path.startswith("/"):
        raise ValueError("relative_path must not be absolute")
    if "\\" in relative_path:
        raise ValueError("relative_path must use forward slashes only")

    path_segments = relative_path.split("/")
    if any(segment in {"", ".", ".."} for segment in path_segments):
        raise ValueError(f"relative_path is not normalized: {relative_path}")

    return relative_path


def _matches_pattern(
    *,
    compiled_pattern: _CompiledExcludedPattern,
    relative_path: str,
) -> bool:
    pattern = compiled_pattern.pattern

    if pattern.pattern_type == "directory_name":
        return pattern.pattern_value in _parent_directory_segments(relative_path)
    if pattern.pattern_type == "glob":
        return fnmatchcase(relative_path, pattern.pattern_value)
    if pattern.pattern_type == "path_substring":
        return pattern.pattern_value in relative_path
    if pattern.pattern_type == "regex":
        compiled_regex = compiled_pattern.compiled_regex
        if compiled_regex is None:
            raise RuntimeError(
                f"Compiled regex is missing for excluded pattern {pattern.id}"
            )
        return compiled_regex.search(relative_path) is not None

    raise RuntimeError(f"Unsupported excluded pattern type: {pattern.pattern_type}")


def _parent_directory_segments(relative_path: str) -> tuple[str, ...]:
    path_segments = relative_path.split("/")
    if len(path_segments) <= 1:
        return ()
    return tuple(path_segments[:-1])
