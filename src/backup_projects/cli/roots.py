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
from backup_projects.services.roots_service import build_roots_page_view


def register(subparsers) -> None:
    parser = subparsers.add_parser("roots", description="List known roots.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)
    nested_subparsers.add_parser("list", description="List known roots.")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List known roots.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)
    nested_subparsers.add_parser("list", description="List known roots.")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            if args.command != "list":
                raise ValueError(f"Unsupported roots command: {args.command}")
            result = build_roots_page_view(session=session, status=None, rescan=None)
            _print_roots(result.rows)
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


def _print_roots(rows) -> None:
    print("Roots")
    if not rows:
        print("- none")
        return

    for row in rows:
        print(f"- id: {row.id}")
        print(f"  raid-name: {row.raid_name}")
        print(f"  name: {row.name}")
        print(f"  path: {row.path}")
        print(f"  status: {row.status}")
        print(
            "  needs-structural-rescan: "
            f"{'yes' if row.needs_structural_rescan else 'no'}"
        )
        print(f"  last-seen-at: {row.last_seen_at}")


if __name__ == "__main__":
    raise SystemExit(main())
