from __future__ import annotations

import argparse
from collections.abc import Sequence

from backup_projects.cli import init_db, seed_default_rules


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="backup-projects command-line interface."
    )
    subparsers = parser.add_subparsers(dest="command")
    _register_commands(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    handler = getattr(args, "_handler", None)
    if handler is None:
        parser.print_help()
        return 0

    return int(handler(args))


def _register_commands(subparsers) -> None:
    init_db.register(subparsers)
    seed_default_rules.register(subparsers)
