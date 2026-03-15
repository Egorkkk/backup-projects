from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ConfigError, load_config
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.roots_repo import RootsRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List manual include rows for one known root.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    root_group = parser.add_mutually_exclusive_group(required=True)
    root_group.add_argument("--root-id", type=int)
    root_group.add_argument("--root-path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            root = _resolve_target_root(
                session=session,
                root_id=args.root_id,
                root_path=args.root_path,
            )
            manual_includes = tuple(ManualIncludesRepository(session).list_by_root(root.id))

        if not manual_includes:
            print(f"No manual includes found for root-id: {root.id}")
            return 0

        _print_manual_includes(manual_includes)
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


def _resolve_target_root(*, session, root_id: int | None, root_path: str | None):
    repo = RootsRepository(session)
    if root_id is not None:
        root = repo.get_by_id(root_id)
        if root is None:
            raise LookupError(f"Root not found for id: {root_id}")
        return root

    if root_path is None:
        raise ValueError("Either --root-id or --root-path must be provided")

    resolved_root_path = resolve_path(root_path).as_posix()
    root = repo.get_by_path(resolved_root_path)
    if root is None:
        raise LookupError(f"Root not found for path: {resolved_root_path}")
    return root


def _print_manual_includes(manual_includes) -> None:
    for index, manual_include in enumerate(manual_includes):
        print(f"id: {manual_include.id}")
        print(f"root-id: {manual_include.root_id}")
        print(f"relative-path: {manual_include.relative_path}")
        print(f"type: {manual_include.include_path_type}")
        print(f"recursive: {str(manual_include.recursive).lower()}")
        print(f"force-include: {str(manual_include.force_include).lower()}")
        print(f"enabled: {str(manual_include.enabled).lower()}")
        if index < len(manual_includes) - 1:
            print()


if __name__ == "__main__":
    raise SystemExit(main())
