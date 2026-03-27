from __future__ import annotations

import argparse
from collections.abc import Sequence

from backup_projects.cli import (
    backup,
    dry_run,
    init_db,
    rules,
    run_daily,
    run_weekly,
    scan_manual,
    scan_project_dirs,
    scan_roots,
    scan_structure,
    seed_default_rules,
)


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
    scan_roots.register(subparsers)
    scan_structure.register(subparsers)
    scan_project_dirs.register(subparsers)
    scan_manual.register(subparsers)
    run_daily.register(subparsers)
    run_weekly.register(subparsers)
    backup.register(subparsers)
    dry_run.register(subparsers)
    rules.register(subparsers)
