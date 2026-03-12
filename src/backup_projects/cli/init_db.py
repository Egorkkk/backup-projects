from __future__ import annotations

import argparse
from collections.abc import Sequence

from backup_projects.adapters.db.bootstrap import initialize_database
from backup_projects.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialize SQLite schema and baseline defaults.")
    parser.add_argument("--app-config", default="config/app.yaml")
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(app_path=args.app_config, rules_path=args.rules_config)
    initialize_database(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
