from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.dry_run_service import build_root_dry_run_manifest


@dataclass(frozen=True, slots=True)
class OversizedSkippedRow:
    root_id: int
    root_path: str
    file_path: str
    extension: str
    size_bytes: int
    oversize_action: str
    warning: str | None


@dataclass(frozen=True, slots=True)
class OversizedSkippedPageView:
    rows: tuple[OversizedSkippedRow, ...]


@dataclass(frozen=True, slots=True)
class UnrecognizedExtensionRow:
    extension: str
    occurrence_count: int
    sample_path: str


@dataclass(frozen=True, slots=True)
class UnrecognizedExtensionsPageView:
    rows: tuple[UnrecognizedExtensionRow, ...]


@dataclass(frozen=True, slots=True)
class ManualOverrideRow:
    root_id: int
    root_path: str
    file_path: str
    extension: str
    size_bytes: int
    reason: str
    manual_include_applied: bool
    force_include_applied: bool


@dataclass(frozen=True, slots=True)
class ManualOverridesPageView:
    rows: tuple[ManualOverrideRow, ...]


def build_oversized_skipped_page_view(*, session: Session) -> OversizedSkippedPageView:
    rows: list[OversizedSkippedRow] = []
    for root, decisions in _iter_active_root_decisions(session=session):
        for decision in decisions:
            if decision.reason != "policy_oversize_skip":
                continue
            rows.append(
                OversizedSkippedRow(
                    root_id=root.id,
                    root_path=root.path,
                    file_path=decision.candidate.absolute_path,
                    extension=_display_extension(decision.candidate.extension),
                    size_bytes=decision.candidate.size_bytes,
                    oversize_action=(
                        decision.oversize_action.value
                        if decision.oversize_action is not None
                        else "-"
                    ),
                    warning=decision.warning,
                )
            )

    return OversizedSkippedPageView(
        rows=tuple(sorted(rows, key=lambda row: (row.root_path, row.file_path)))
    )


def build_unrecognized_extensions_page_view(
    *,
    session: Session,
) -> UnrecognizedExtensionsPageView:
    aggregated: dict[str, UnrecognizedExtensionRow] = {}

    for _root, decisions in _iter_active_root_decisions(session=session):
        for decision in decisions:
            if not _is_unrecognized_extension_decision(decision):
                continue

            extension = _display_extension(decision.candidate.extension)
            existing = aggregated.get(extension)
            if existing is None:
                aggregated[extension] = UnrecognizedExtensionRow(
                    extension=extension,
                    occurrence_count=1,
                    sample_path=decision.candidate.absolute_path,
                )
                continue

            sample_path = min(existing.sample_path, decision.candidate.absolute_path)
            aggregated[extension] = UnrecognizedExtensionRow(
                extension=extension,
                occurrence_count=existing.occurrence_count + 1,
                sample_path=sample_path,
            )

    return UnrecognizedExtensionsPageView(
        rows=tuple(sorted(aggregated.values(), key=lambda row: row.extension))
    )


def build_manual_overrides_page_view(*, session: Session) -> ManualOverridesPageView:
    rows: list[ManualOverrideRow] = []
    for root, decisions in _iter_active_root_decisions(session=session):
        for decision in decisions:
            if not decision.force_include_applied:
                continue
            rows.append(
                ManualOverrideRow(
                    root_id=root.id,
                    root_path=root.path,
                    file_path=decision.candidate.absolute_path,
                    extension=_display_extension(decision.candidate.extension),
                    size_bytes=decision.candidate.size_bytes,
                    reason=decision.reason,
                    manual_include_applied=decision.manual_include_applied,
                    force_include_applied=decision.force_include_applied,
                )
            )

    return ManualOverridesPageView(
        rows=tuple(sorted(rows, key=lambda row: (row.root_path, row.file_path)))
    )


def _iter_active_root_decisions(*, session: Session):
    roots = RootsRepository(session).list_active()
    for root in roots:
        built_manifest = build_root_dry_run_manifest(session=session, root_id=root.id)
        yield root, built_manifest.decisions


def _is_unrecognized_extension_decision(decision) -> bool:
    return (
        decision.reason == "policy_unknown_extension_skip"
        or decision.warning == "unknown_extension"
    )


def _display_extension(extension: str) -> str:
    return extension if extension != "" else "(none)"
