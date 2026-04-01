from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import join_path, resolve_path
from backup_projects.domain.enums import IncludePathType
from backup_projects.domain.models import CandidateFile
from backup_projects.repositories.manual_includes_repo import (
    ManualIncludeRecord,
    ManualIncludesRepository,
)
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import (
    ProjectFileRecord,
    ProjectFilesRepository,
)
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.decision_engine import (
    DecisionCandidate,
    build_decision_engine,
)
from backup_projects.services.manifest_builder import BuiltManifest, build_manifest
from backup_projects.services.rule_loader import load_policy_config


@dataclass(frozen=True, slots=True)
class _MatchedManualInclude:
    manual_include_id: int
    force_include: bool


@dataclass(frozen=True, slots=True)
class RootDryRunManifestPlan:
    root_id: int
    status: str
    built_manifest: BuiltManifest | None = None
    error: str | None = None

    @property
    def included_count(self) -> int:
        if self.built_manifest is None:
            return 0
        return sum(1 for decision in self.built_manifest.decisions if decision.include)

    @property
    def skipped_count(self) -> int:
        if self.built_manifest is None:
            return 0
        return sum(1 for decision in self.built_manifest.decisions if not decision.include)


@dataclass(frozen=True, slots=True)
class MultiRootDryRunManifestPlan:
    root_plans: tuple[RootDryRunManifestPlan, ...]
    built_manifest: BuiltManifest


def build_root_dry_run_manifest(*, session: Session, root_id: int) -> BuiltManifest:
    roots_repo = RootsRepository(session)
    project_dirs_repo = ProjectDirsRepository(session)
    project_files_repo = ProjectFilesRepository(session)
    manual_includes_repo = ManualIncludesRepository(session)

    root = roots_repo.get_by_id(root_id)
    if root is None:
        raise LookupError(f"Root not found for id: {root_id}")

    policy_config = load_policy_config(session=session)
    decision_engine = build_decision_engine(policy_config=policy_config)

    project_dirs = project_dirs_repo.list_active_by_root(root.id)
    manual_includes = tuple(manual_includes_repo.list_enabled_by_root(root.id))
    decisions = []

    for project_dir in project_dirs:
        project_files = project_files_repo.list_by_project_dir(project_dir.id)
        for project_file in project_files:
            if project_file.is_missing:
                continue
            decisions.append(
                decision_engine.evaluate_candidate(
                    candidate=_build_decision_candidate(
                        root_path=root.path,
                        project_file=project_file,
                        manual_includes=manual_includes,
                    )
                )
            )

    return build_manifest(decisions=decisions)


def build_multi_root_dry_run_manifest(
    *,
    session: Session,
    root_ids: Sequence[int],
) -> MultiRootDryRunManifestPlan:
    root_plans: list[RootDryRunManifestPlan] = []
    combined_decisions = []

    for root_id in root_ids:
        try:
            built_manifest = build_root_dry_run_manifest(
                session=session,
                root_id=root_id,
            )
        except Exception as exc:
            root_plans.append(
                RootDryRunManifestPlan(
                    root_id=root_id,
                    status="failed",
                    error=str(exc),
                )
            )
            continue

        root_plans.append(
            RootDryRunManifestPlan(
                root_id=root_id,
                status="completed",
                built_manifest=built_manifest,
            )
        )
        combined_decisions.extend(built_manifest.decisions)

    return MultiRootDryRunManifestPlan(
        root_plans=tuple(root_plans),
        built_manifest=build_manifest(decisions=combined_decisions),
    )


def _build_decision_candidate(
    *,
    root_path: str,
    project_file: ProjectFileRecord,
    manual_includes: tuple[ManualIncludeRecord, ...],
) -> DecisionCandidate:
    matched_manual_include = _match_manual_include(
        file_relative_path=project_file.relative_path,
        manual_includes=manual_includes,
    )
    absolute_path = resolve_path(
        join_path(root_path, project_file.relative_path)
    ).as_posix()

    return DecisionCandidate(
        candidate=CandidateFile(
            absolute_path=absolute_path,
            extension=project_file.extension,
            size_bytes=project_file.size_bytes,
            mtime_ns=project_file.mtime_ns,
            ctime_ns=project_file.ctime_ns,
            inode=project_file.inode,
            project_dir_id=project_file.project_dir_id,
            project_file_id=project_file.id,
            manual_include_id=(
                matched_manual_include.manual_include_id
                if matched_manual_include is not None
                else None
            ),
        ),
        relative_path=project_file.relative_path,
        force_include=(
            matched_manual_include.force_include
            if matched_manual_include is not None
            else False
        ),
    )


def _match_manual_include(
    *,
    file_relative_path: str,
    manual_includes: tuple[ManualIncludeRecord, ...],
) -> _MatchedManualInclude | None:
    matching_includes = [
        manual_include
        for manual_include in manual_includes
        if _manual_include_matches(
            file_relative_path=file_relative_path,
            manual_include=manual_include,
        )
    ]
    if not matching_includes:
        return None

    provenance_include = min(
        matching_includes,
        key=lambda manual_include: (
            0
            if manual_include.include_path_type == IncludePathType.FILE.value
            else 1,
            -len(manual_include.relative_path),
            manual_include.id,
        ),
    )

    return _MatchedManualInclude(
        manual_include_id=provenance_include.id,
        force_include=any(
            manual_include.force_include for manual_include in matching_includes
        ),
    )


def _manual_include_matches(
    *,
    file_relative_path: str,
    manual_include: ManualIncludeRecord,
) -> bool:
    include_path_type = IncludePathType(manual_include.include_path_type)

    if include_path_type is IncludePathType.FILE:
        return manual_include.relative_path == file_relative_path

    if include_path_type is not IncludePathType.DIRECTORY:
        raise ValueError(
            f"Invalid include_path_type: {manual_include.include_path_type}"
        )

    include_relative_path = manual_include.relative_path
    if include_relative_path == "":
        return manual_include.recursive or "/" not in file_relative_path

    if manual_include.recursive:
        return file_relative_path.startswith(f"{include_relative_path}/")

    direct_parent = (
        file_relative_path.rsplit("/", 1)[0] if "/" in file_relative_path else ""
    )
    return direct_parent == include_relative_path
