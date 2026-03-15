from __future__ import annotations

from dataclasses import dataclass

from backup_projects.domain.models import CandidateFile, FinalDecision
from backup_projects.services.exclude_matcher import (
    ExcludeMatch,
    ExcludeMatcher,
    build_exclude_matcher,
)
from backup_projects.services.extension_policy_service import (
    ExtensionPolicyEvaluator,
    ExtensionPolicyResult,
    build_extension_policy_evaluator,
)
from backup_projects.services.rule_loader import LoadedPolicyConfig


@dataclass(frozen=True, slots=True)
class DecisionCandidate:
    candidate: CandidateFile
    relative_path: str
    force_include: bool = False


@dataclass(frozen=True, slots=True)
class _DecisionContext:
    candidate: DecisionCandidate
    manual_include_applied: bool
    exclude_match: ExcludeMatch | None
    extension_policy_result: ExtensionPolicyResult


@dataclass(frozen=True, slots=True)
class DecisionEngine:
    _exclude_matcher: ExcludeMatcher
    _extension_policy_evaluator: ExtensionPolicyEvaluator

    def evaluate_candidate(self, *, candidate: DecisionCandidate) -> FinalDecision:
        _validate_decision_candidate(candidate)

        exclude_match = self._exclude_matcher.match_path(
            relative_path=candidate.relative_path
        )
        extension_policy_result = self._extension_policy_evaluator.evaluate_candidate(
            relative_path=candidate.relative_path,
            size_bytes=candidate.candidate.size_bytes,
        )

        context = _DecisionContext(
            candidate=candidate,
            manual_include_applied=candidate.candidate.manual_include_id is not None,
            exclude_match=exclude_match,
            extension_policy_result=extension_policy_result,
        )

        return _compose_final_decision(context)


def build_decision_engine(*, policy_config: LoadedPolicyConfig) -> DecisionEngine:
    return DecisionEngine(
        _exclude_matcher=build_exclude_matcher(policy_config=policy_config),
        _extension_policy_evaluator=build_extension_policy_evaluator(
            policy_config=policy_config
        ),
    )


def _validate_decision_candidate(candidate: DecisionCandidate) -> None:
    if candidate.relative_path == "":
        raise ValueError("DecisionCandidate.relative_path must not be empty")
    if candidate.force_include and candidate.candidate.manual_include_id is None:
        raise ValueError(
            "DecisionCandidate.force_include requires candidate.manual_include_id"
        )


def _compose_final_decision(context: _DecisionContext) -> FinalDecision:
    if context.candidate.force_include:
        return _compose_force_include_decision(context)
    if context.exclude_match is not None:
        return _build_final_decision(
            context,
            include=False,
            reason="excluded",
            warning=_derive_warning(context.extension_policy_result),
        )
    return _build_policy_driven_decision(context)


def _compose_force_include_decision(context: _DecisionContext) -> FinalDecision:
    if context.exclude_match is not None:
        return _build_final_decision(
            context,
            include=True,
            reason="force_include_override_exclude",
            warning=_derive_warning(context.extension_policy_result),
        )

    extension_policy_result = context.extension_policy_result
    if extension_policy_result.policy_allows_candidate:
        return _build_policy_driven_decision(context)

    if extension_policy_result.extension_known:
        return _build_final_decision(
            context,
            include=True,
            reason="force_include_override_policy_oversize",
            warning=_derive_warning(extension_policy_result),
        )

    return _build_final_decision(
        context,
        include=True,
        reason="force_include_override_policy_unknown_extension",
        warning=_derive_warning(extension_policy_result),
    )


def _build_policy_driven_decision(context: _DecisionContext) -> FinalDecision:
    extension_policy_result = context.extension_policy_result

    return _build_final_decision(
        context,
        include=extension_policy_result.policy_allows_candidate,
        reason=_map_policy_reason(extension_policy_result),
        warning=_derive_warning(extension_policy_result),
    )


def _build_final_decision(
    context: _DecisionContext,
    *,
    include: bool,
    reason: str,
    warning: str | None,
) -> FinalDecision:
    extension_policy_result = context.extension_policy_result

    return FinalDecision(
        candidate=context.candidate.candidate,
        include=include,
        reason=reason,
        oversize_action=extension_policy_result.oversize_action,
        warning=warning,
        manual_include_applied=context.manual_include_applied,
        force_include_applied=context.candidate.force_include,
        exclude_pattern=(
            context.exclude_match.pattern if context.exclude_match is not None else None
        ),
        extension_policy_reason=extension_policy_result.reason,
        should_store_unknown_extension=(
            extension_policy_result.should_store_unknown_extension
        ),
        should_log_unknown_extension_warning=(
            extension_policy_result.should_log_unknown_extension_warning
        ),
    )


def _map_policy_reason(extension_policy_result: ExtensionPolicyResult) -> str:
    if extension_policy_result.reason == "known_extension_allowed":
        return "policy_include"
    if extension_policy_result.reason == "known_extension_oversize_warn":
        return "policy_oversize_warn_include"
    if extension_policy_result.reason == "known_extension_oversize_include":
        return "policy_oversize_include"
    if extension_policy_result.reason == "known_extension_oversize_skip":
        return "policy_oversize_skip"
    if extension_policy_result.reason in {
        "unknown_extension_collect_and_skip",
        "unknown_extension_skip_silent",
    }:
        return "policy_unknown_extension_skip"

    raise ValueError(
        f"Unsupported extension policy reason: {extension_policy_result.reason}"
    )


def _derive_warning(extension_policy_result: ExtensionPolicyResult) -> str | None:
    if not extension_policy_result.should_warn:
        return None
    if not extension_policy_result.extension_known:
        return "unknown_extension"
    if extension_policy_result.is_oversize:
        return "oversize"
    return None
