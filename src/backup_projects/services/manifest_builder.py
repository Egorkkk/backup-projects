from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from backup_projects.domain.models import FinalDecision, ManifestResult


@dataclass(frozen=True, slots=True)
class BuiltManifest:
    manifest_paths: tuple[str, ...]
    decisions: tuple[FinalDecision, ...]
    json_payload: dict[str, object]
    summary_text: str


def build_manifest(*, decisions: Iterable[FinalDecision]) -> BuiltManifest:
    ordered_decisions = _normalize_decisions(decisions)
    manifest_paths = tuple(
        decision.candidate.absolute_path
        for decision in ordered_decisions
        if decision.include
    )
    json_payload = _build_json_payload(ordered_decisions)
    summary_text = _build_summary_text(ordered_decisions)

    return BuiltManifest(
        manifest_paths=manifest_paths,
        decisions=ordered_decisions,
        json_payload=json_payload,
        summary_text=summary_text,
    )


def write_manifest(
    *,
    built_manifest: BuiltManifest,
    output_dir: Path,
    artifact_stem: str,
) -> ManifestResult:
    normalized_output_dir = _validate_output_dir(output_dir)
    normalized_artifact_stem = _validate_artifact_stem(artifact_stem)

    manifest_file_path = (
        normalized_output_dir / f"{normalized_artifact_stem}.manifest.txt"
    )
    json_manifest_file_path = (
        normalized_output_dir / f"{normalized_artifact_stem}.manifest.json"
    )
    summary_file_path = (
        normalized_output_dir / f"{normalized_artifact_stem}.summary.txt"
    )

    manifest_file_path.write_text(
        _render_manifest_paths(built_manifest.manifest_paths),
        encoding="utf-8",
    )
    json_manifest_file_path.write_text(
        json.dumps(built_manifest.json_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    summary_file_path.write_text(built_manifest.summary_text + "\n", encoding="utf-8")

    return ManifestResult(
        manifest_paths=built_manifest.manifest_paths,
        decisions=built_manifest.decisions,
        manifest_file_path=str(manifest_file_path),
        json_manifest_file_path=str(json_manifest_file_path),
        summary_file_path=str(summary_file_path),
    )


def _normalize_decisions(
    decisions: Iterable[FinalDecision],
) -> tuple[FinalDecision, ...]:
    ordered_decisions = tuple(
        sorted(decisions, key=lambda decision: decision.candidate.absolute_path)
    )
    seen_paths: set[str] = set()

    for decision in ordered_decisions:
        absolute_path = decision.candidate.absolute_path
        if absolute_path == "":
            raise ValueError("FinalDecision.candidate.absolute_path must not be empty")
        if absolute_path in seen_paths:
            raise ValueError(
                f"Duplicate candidate.absolute_path detected: {absolute_path}"
            )
        seen_paths.add(absolute_path)

    return ordered_decisions


def _build_json_payload(decisions: tuple[FinalDecision, ...]) -> dict[str, object]:
    included_decisions = tuple(decision for decision in decisions if decision.include)
    skipped_counts_by_reason = Counter(
        decision.reason for decision in decisions if not decision.include
    )
    warning_counts_by_type = Counter(
        decision.warning for decision in decisions if decision.warning is not None
    )

    included_files = [
        {
            "path": decision.candidate.absolute_path,
            "size_bytes": decision.candidate.size_bytes,
            "reason": decision.reason,
            "warning": decision.warning,
            "manual_include_applied": decision.manual_include_applied,
            "force_include_applied": decision.force_include_applied,
            "oversize_action": _serialize_oversize_action(decision),
            "extension_policy_reason": decision.extension_policy_reason,
        }
        for decision in included_decisions
    ]

    return {
        "format_version": 1,
        "counts": {
            "total_decisions": len(decisions),
            "included": len(included_decisions),
            "skipped": len(decisions) - len(included_decisions),
            "warnings": sum(warning_counts_by_type.values()),
            "included_bytes": sum(
                decision.candidate.size_bytes for decision in included_decisions
            ),
        },
        "included_files": included_files,
        "skipped_counts_by_reason": dict(sorted(skipped_counts_by_reason.items())),
        "warning_counts_by_type": dict(sorted(warning_counts_by_type.items())),
    }


def _build_summary_text(decisions: tuple[FinalDecision, ...]) -> str:
    included_count = sum(1 for decision in decisions if decision.include)
    warning_counts = Counter(
        decision.warning for decision in decisions if decision.warning is not None
    )
    reason_counts = Counter(decision.reason for decision in decisions)
    lines = [
        "Manifest summary",
        f"Total decisions: {len(decisions)}",
        f"Included: {included_count}",
        f"Skipped: {len(decisions) - included_count}",
        f"Warnings: {sum(warning_counts.values())}",
        "Included bytes: "
        f"{sum(decision.candidate.size_bytes for decision in decisions if decision.include)}",
        "",
        "Reason counts:",
        *_render_counter_lines(reason_counts),
        "",
        "Warning counts:",
        *_render_counter_lines(warning_counts),
    ]
    return "\n".join(lines)


def _render_counter_lines(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none: 0"]
    return [f"- {key}: {counter[key]}" for key in sorted(counter)]


def _serialize_oversize_action(decision: FinalDecision) -> str | None:
    if decision.oversize_action is None:
        return None
    return decision.oversize_action.value


def _render_manifest_paths(manifest_paths: tuple[str, ...]) -> str:
    if not manifest_paths:
        return ""
    return "\n".join(manifest_paths) + "\n"


def _validate_output_dir(output_dir: Path) -> Path:
    if not output_dir.exists():
        raise ValueError(f"output_dir does not exist: {output_dir}")
    if not output_dir.is_dir():
        raise ValueError(f"output_dir is not a directory: {output_dir}")
    return output_dir


def _validate_artifact_stem(artifact_stem: str) -> str:
    normalized_artifact_stem = artifact_stem.strip()
    if normalized_artifact_stem == "":
        raise ValueError("artifact_stem must not be empty")
    return normalized_artifact_stem
