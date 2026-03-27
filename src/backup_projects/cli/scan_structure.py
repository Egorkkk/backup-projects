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
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ConfigError, load_config
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.structural_scan_service import scan_root_structure
from backup_projects.services.structural_scan_sync_service import (
    StructuralScanSyncResult,
    sync_structural_scan_result,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "scan-structure",
        description="Run structural scan for one known root and sync results to SQLite.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--root-id", type=int)
    target_group.add_argument("--path")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run structural scan for one known root and sync results to SQLite."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--root-id", type=int)
    target_group.add_argument("--path")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            root = _resolve_target_root(
                session=session,
                root_id=args.root_id,
                path=args.path,
            )
            scan_result = scan_root_structure(
                root_path=root.path,
                allowed_extensions=config.rules_config.allowed_extensions,
            )
            synced_at = datetime.now(timezone.utc).isoformat()
            result = sync_structural_scan_result(
                session=session,
                root_id=root.id,
                scan_result=scan_result,
                synced_at=synced_at,
            )

        _print_summary(root.name, root.path, result)
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


def _resolve_target_root(*, session, root_id: int | None, path: str | None):
    repo = RootsRepository(session)
    if root_id is not None:
        root = repo.get_by_id(root_id)
        if root is None:
            raise LookupError(f"Root not found for id: {root_id}")
        return root

    if path is None:
        raise ValueError("Either --root-id or --path must be provided")

    resolved_path = resolve_path(path).as_posix()
    root = repo.get_by_path(resolved_path)
    if root is None:
        raise LookupError(f"Root not found for path: {resolved_path}")
    return root


def _print_summary(root_name: str, root_path: str, result: StructuralScanSyncResult) -> None:
    print(f"Scanned target: {root_name} ({root_path})")
    print(f"root-id: {result.root_id}")
    print(f"project-dirs-scanned: {result.scanned_project_dir_count}")
    print(f"project-dirs-created: {result.created_project_dir_count}")
    print(f"project-dirs-updated: {result.updated_project_dir_count}")
    print(f"project-dirs-reactivated: {result.reactivated_project_dir_count}")
    print(f"project-dirs-marked-missing: {result.marked_missing_project_dir_count}")
    print(f"project-files-scanned: {result.scanned_project_file_count}")
    print(f"project-files-created: {result.created_project_file_count}")
    print(f"project-files-updated: {result.updated_project_file_count}")
    print(f"project-files-reactivated: {result.reactivated_project_file_count}")
    print(f"project-files-marked-missing: {result.marked_missing_project_file_count}")


if __name__ == "__main__":
    raise SystemExit(main())
