from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.services.project_dirs_service import build_project_dirs_page_view


def register(subparsers) -> None:
    parser = subparsers.add_parser("dirs", description="List known project directories.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)
    nested_subparsers.add_parser("list", description="List known project directories.")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List known project directories.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)
    nested_subparsers.add_parser("list", description="List known project directories.")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            if args.command != "list":
                raise ValueError(f"Unsupported dirs command: {args.command}")
            result = build_project_dirs_page_view(session=session)
            _print_dirs(result.rows)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except (LookupError, ValueError) as exc:
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


def _print_dirs(rows) -> None:
    print("Project directories")
    if not rows:
        print("- none")
        return

    for row in rows:
        print(f"- id: {row.id}")
        print(f"  root-id: {row.root_id}")
        print(f"  root-name: {row.root_name}")
        print(f"  root-path: {row.root_path}")
        print(f"  relative-path: {row.relative_path}")
        print(f"  name: {row.name}")
        print(f"  dir-type: {row.dir_type}")
        print(f"  status: {row.status}")
        print(f"  last-seen-at: {row.last_seen_at}")


if __name__ == "__main__":
    raise SystemExit(main())
