from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from backup_projects.adapters.db.bootstrap import seed_default_rules
from backup_projects.config import ConfigError, load_config


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "seed-default-rules",
        description="Seed default settings and policy rules.",
    )
    parser.add_argument("--app-config", default="config/app.yaml")
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed default settings and policy rules."
    )
    parser.add_argument("--app-config", default="config/app.yaml")
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.app_config, rules_path=args.rules_config)
        seed_default_rules(config)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print("Default settings and rules seeded.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


if __name__ == "__main__":
    raise SystemExit(main())
