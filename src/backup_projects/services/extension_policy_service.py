from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from backup_projects.constants import UnknownExtensionAction
from backup_projects.domain.enums import OversizeAction
from backup_projects.domain.models import ExtensionRule
from backup_projects.services.rule_loader import LoadedPolicyConfig


@dataclass(frozen=True, slots=True)
class ExtensionPolicyResult:
    relative_path: str
    extension: str
    matched_rule: ExtensionRule | None
    extension_known: bool
    size_bytes: int
    is_oversize: bool
    max_size_bytes: int | None
    oversize_action: OversizeAction | None
    policy_allows_candidate: bool
    should_warn: bool
    should_store_unknown_extension: bool
    should_log_unknown_extension_warning: bool
    reason: str


@dataclass(frozen=True, slots=True)
class ExtensionPolicyEvaluator:
    _rules_by_extension: dict[str, ExtensionRule]
    _unknown_extension_action: UnknownExtensionAction
    _oversize_log_skipped: bool
    _unknown_extensions_log_warning: bool

    def evaluate_candidate(
        self,
        *,
        relative_path: str,
        size_bytes: int,
    ) -> ExtensionPolicyResult:
        normalized_relative_path = _validate_relative_path(relative_path)
        normalized_size_bytes = _validate_size_bytes(size_bytes)
        extension = _extract_extension(normalized_relative_path)
        matched_rule = self._rules_by_extension.get(extension)

        if matched_rule is None:
            return self._build_unknown_extension_result(
                relative_path=normalized_relative_path,
                extension=extension,
                size_bytes=normalized_size_bytes,
            )

        return _evaluate_known_extension(
            relative_path=normalized_relative_path,
            extension=extension,
            size_bytes=normalized_size_bytes,
            matched_rule=matched_rule,
            oversize_log_skipped=self._oversize_log_skipped,
        )

    def _build_unknown_extension_result(
        self,
        *,
        relative_path: str,
        extension: str,
        size_bytes: int,
    ) -> ExtensionPolicyResult:
        if self._unknown_extension_action is UnknownExtensionAction.COLLECT_AND_SKIP:
            should_warn = self._unknown_extensions_log_warning
            return ExtensionPolicyResult(
                relative_path=relative_path,
                extension=extension,
                matched_rule=None,
                extension_known=False,
                size_bytes=size_bytes,
                is_oversize=False,
                max_size_bytes=None,
                oversize_action=None,
                policy_allows_candidate=False,
                should_warn=should_warn,
                should_store_unknown_extension=True,
                should_log_unknown_extension_warning=self._unknown_extensions_log_warning,
                reason="unknown_extension_collect_and_skip",
            )

        return ExtensionPolicyResult(
            relative_path=relative_path,
            extension=extension,
            matched_rule=None,
            extension_known=False,
            size_bytes=size_bytes,
            is_oversize=False,
            max_size_bytes=None,
            oversize_action=None,
            policy_allows_candidate=False,
            should_warn=False,
            should_store_unknown_extension=False,
            should_log_unknown_extension_warning=False,
            reason="unknown_extension_skip_silent",
        )


def build_extension_policy_evaluator(
    *,
    policy_config: LoadedPolicyConfig,
) -> ExtensionPolicyEvaluator:
    unknown_extension_action = _validate_unknown_extension_action(
        policy_config.settings.unknown_extensions_action
    )
    rules_by_extension = _build_rules_by_extension(policy_config.extension_rules)

    return ExtensionPolicyEvaluator(
        _rules_by_extension=rules_by_extension,
        _unknown_extension_action=unknown_extension_action,
        _oversize_log_skipped=policy_config.settings.oversize_log_skipped,
        _unknown_extensions_log_warning=policy_config.settings.unknown_extensions_log_warning,
    )


def _build_rules_by_extension(
    extension_rules: tuple[ExtensionRule, ...],
) -> dict[str, ExtensionRule]:
    rules_by_extension: dict[str, ExtensionRule] = {}

    for rule in extension_rules:
        normalized_extension = _normalize_extension(rule.extension)
        if normalized_extension == "":
            raise ValueError(f"Extension rule {rule.id} has an empty normalized extension")
        if normalized_extension in rules_by_extension:
            raise ValueError(
                f"Duplicate normalized extension rule detected: {normalized_extension}"
            )
        rules_by_extension[normalized_extension] = rule

    return rules_by_extension


def _validate_unknown_extension_action(value: str) -> UnknownExtensionAction:
    try:
        return UnknownExtensionAction(value)
    except ValueError as exc:
        raise ValueError(f"Invalid unknown_extensions.action: {value}") from exc


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


def _validate_size_bytes(size_bytes: int) -> int:
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
        raise ValueError(f"size_bytes must be int, got {type(size_bytes).__name__}")
    if size_bytes < 0:
        raise ValueError("size_bytes must be >= 0")
    return size_bytes


def _extract_extension(relative_path: str) -> str:
    return _normalize_extension(PurePosixPath(relative_path).suffix)


def _normalize_extension(extension: str) -> str:
    normalized_extension = extension.strip().lower()
    if normalized_extension.startswith("."):
        normalized_extension = normalized_extension[1:]
    return normalized_extension


def _evaluate_known_extension(
    *,
    relative_path: str,
    extension: str,
    size_bytes: int,
    matched_rule: ExtensionRule,
    oversize_log_skipped: bool,
) -> ExtensionPolicyResult:
    max_size_bytes = matched_rule.max_size_bytes
    if max_size_bytes is None or size_bytes <= max_size_bytes:
        return ExtensionPolicyResult(
            relative_path=relative_path,
            extension=extension,
            matched_rule=matched_rule,
            extension_known=True,
            size_bytes=size_bytes,
            is_oversize=False,
            max_size_bytes=max_size_bytes,
            oversize_action=None,
            policy_allows_candidate=True,
            should_warn=False,
            should_store_unknown_extension=False,
            should_log_unknown_extension_warning=False,
            reason="known_extension_allowed",
        )

    oversize_action = matched_rule.oversize_action
    if oversize_action is OversizeAction.SKIP:
        return ExtensionPolicyResult(
            relative_path=relative_path,
            extension=extension,
            matched_rule=matched_rule,
            extension_known=True,
            size_bytes=size_bytes,
            is_oversize=True,
            max_size_bytes=max_size_bytes,
            oversize_action=oversize_action,
            policy_allows_candidate=False,
            should_warn=oversize_log_skipped,
            should_store_unknown_extension=False,
            should_log_unknown_extension_warning=False,
            reason="known_extension_oversize_skip",
        )
    if oversize_action is OversizeAction.WARN:
        return ExtensionPolicyResult(
            relative_path=relative_path,
            extension=extension,
            matched_rule=matched_rule,
            extension_known=True,
            size_bytes=size_bytes,
            is_oversize=True,
            max_size_bytes=max_size_bytes,
            oversize_action=oversize_action,
            policy_allows_candidate=True,
            should_warn=True,
            should_store_unknown_extension=False,
            should_log_unknown_extension_warning=False,
            reason="known_extension_oversize_warn",
        )
    if oversize_action is OversizeAction.INCLUDE:
        return ExtensionPolicyResult(
            relative_path=relative_path,
            extension=extension,
            matched_rule=matched_rule,
            extension_known=True,
            size_bytes=size_bytes,
            is_oversize=True,
            max_size_bytes=max_size_bytes,
            oversize_action=oversize_action,
            policy_allows_candidate=True,
            should_warn=False,
            should_store_unknown_extension=False,
            should_log_unknown_extension_warning=False,
            reason="known_extension_oversize_include",
        )

    raise ValueError(
        f"Unsupported oversize_action for extension rule {matched_rule.id}: "
        f"{matched_rule.oversize_action}"
    )
