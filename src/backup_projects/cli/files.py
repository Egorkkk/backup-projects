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
from backup_projects.services.skipped_files_service import list_skipped_files


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect current file visibility decisions.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_skipped_parser = subparsers.add_parser(
        "list-skipped",
        description="List skipped file paths for one current root.",
    )
    root_group = list_skipped_parser.add_mutually_exclusive_group(required=True)
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
            if args.command != "list-skipped":
                raise ValueError(f"Unsupported files command: {args.command}")

            result = list_skipped_files(
                session=session,
                root_id=args.root_id,
                root_path=args.root_path,
            )
            _print_skipped_files(result)
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


def _print_skipped_files(result) -> None:
    print(f"Skipped files for root-id: {result.root_id}")
    print(f"root-path: {result.root_path}")
    if not result.skipped_files:
        print("- none")
        return
    for skipped_file in result.skipped_files:
        print(f"- {skipped_file.path}")


if __name__ == "__main__":
    raise SystemExit(main())
