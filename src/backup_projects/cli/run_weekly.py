from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "run-weekly",
        description="Placeholder weekly CLI entrypoint.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Placeholder weekly CLI entrypoint."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def handle(args: argparse.Namespace) -> int:
    _ = args
    print("run-weekly is not implemented in v1 baseline", file=sys.stderr)
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


if __name__ == "__main__":
    raise SystemExit(main())
