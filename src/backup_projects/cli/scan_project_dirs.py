from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.project_dir_scan_service import (
    ProjectDirIncrementalScanResult,
    scan_and_sync_project_dir,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "scan-project-dirs",
        description="Run incremental scan for known project dirs and sync results to SQLite.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--project-dir-id", type=int)
    target_group.add_argument("--root-id", type=int)
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run incremental scan for known project dirs and sync results to SQLite."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--project-dir-id", type=int)
    target_group.add_argument("--root-id", type=int)
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)
        no_targets_root_id: int | None = None

        with session_scope(session_factory) as session:
            target_project_dirs = _resolve_target_project_dirs(
                session=session,
                project_dir_id=args.project_dir_id,
                root_id=args.root_id,
            )
            if not target_project_dirs:
                no_targets_root_id = args.root_id
            else:
                scanned_at = datetime.now(timezone.utc).isoformat()
                results = tuple(
                    scan_and_sync_project_dir(
                        session=session,
                        project_dir_id=project_dir.id,
                        scanned_at=scanned_at,
                    )
                    for project_dir in target_project_dirs
                )

        if no_targets_root_id is not None:
            _print_no_targets_message(no_targets_root_id)
            return 0
        _print_summaries(results)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        engine = locals().get("engine")
        if engine is not None:
            engine.dispose()

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


def _resolve_target_project_dirs(*, session, project_dir_id: int | None, root_id: int | None):
    project_dirs_repo = ProjectDirsRepository(session)
    if project_dir_id is not None:
        project_dir = project_dirs_repo.get_by_id(project_dir_id)
        if project_dir is None:
            raise LookupError(f"Project dir not found for id: {project_dir_id}")
        return (project_dir,)

    if root_id is None:
        raise ValueError("Either --project-dir-id or --root-id must be provided")

    root = RootsRepository(session).get_by_id(root_id)
    if root is None:
        raise LookupError(f"Root not found for id: {root_id}")
    return tuple(project_dirs_repo.list_active_by_root(root_id))


def _print_no_targets_message(root_id: int | None) -> None:
    print(f"No active project dirs found for root-id: {root_id}")


def _print_summaries(results: tuple[ProjectDirIncrementalScanResult, ...]) -> None:
    for index, result in enumerate(results):
        target_label = result.project_dir_relative_path or "root"
        print(f"Scanned target: {target_label}")
        print(f"project-dir-id: {result.project_dir_id}")
        print(f"root-id: {result.root_id}")
        print(f"project-dir-path: {result.project_dir_path}")
        print(f"project-dir-present: {str(result.project_dir_present).lower()}")
        print(f"files-scanned: {result.scanned_file_count}")
        print(f"files-new: {result.new_file_count}")
        print(f"files-changed: {result.changed_file_count}")
        print(f"files-reactivated: {result.reactivated_file_count}")
        print(f"files-missing: {result.missing_file_count}")
        print(f"files-unchanged: {result.unchanged_file_count}")
        if index < len(results) - 1:
            print()


if __name__ == "__main__":
    raise SystemExit(main())
