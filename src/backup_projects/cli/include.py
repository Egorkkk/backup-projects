from __future__ import annotations

import argparse
from collections.abc import Sequence

from backup_projects.cli import (
    include_add_dir,
    include_add_file,
    include_disable,
    include_enable,
    include_list,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "include",
        description="Create, list, enable, and disable manual includes.",
    )
    _configure_parser(parser)
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create, list, enable, and disable manual includes."
    )
    _configure_parser(parser)
    return parser


def handle(args: argparse.Namespace) -> int:
    if args.command == "add-file":
        return include_add_file.main(_build_add_file_argv(args))
    if args.command == "add-dir":
        return include_add_dir.main(_build_add_dir_argv(args))
    if args.command == "list":
        return include_list.main(_build_list_argv(args))
    if args.command == "disable":
        return include_disable.main(_build_disable_argv(args))
    if args.command == "enable":
        return include_enable.main(_build_enable_argv(args))
    raise ValueError(f"Unsupported include command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)

    add_file_parser = nested_subparsers.add_parser(
        "add-file",
        description="Create one manual file include row in SQLite.",
    )
    _add_root_selector(add_file_parser)
    add_file_parser.add_argument("--force-include", action="store_true")
    add_file_parser.add_argument("--disabled", action="store_true")
    add_file_parser.add_argument("target_path")

    add_dir_parser = nested_subparsers.add_parser(
        "add-dir",
        description="Create one manual directory include row in SQLite.",
    )
    _add_root_selector(add_dir_parser)
    add_dir_parser.add_argument("--recursive", action="store_true")
    add_dir_parser.add_argument("--force-include", action="store_true")
    add_dir_parser.add_argument("--disabled", action="store_true")
    add_dir_parser.add_argument("target_path")

    list_parser = nested_subparsers.add_parser(
        "list",
        description="List manual include rows for one known root.",
    )
    _add_root_selector(list_parser)

    disable_parser = nested_subparsers.add_parser(
        "disable",
        description="Disable one manual include row in SQLite.",
    )
    disable_parser.add_argument("--manual-include-id", required=True, type=int)

    enable_parser = nested_subparsers.add_parser(
        "enable",
        description="Enable one manual include row in SQLite.",
    )
    enable_parser.add_argument("--manual-include-id", required=True, type=int)


def _add_root_selector(parser: argparse.ArgumentParser) -> None:
    root_group = parser.add_mutually_exclusive_group(required=True)
    root_group.add_argument("--root-id", type=int)
    root_group.add_argument("--root-path")


def _build_common_argv(args: argparse.Namespace) -> list[str]:
    argv = ["--config", args.config]
    if args.rules_config != "config/rules.yaml":
        argv.extend(["--rules-config", args.rules_config])
    return argv


def _append_root_selector(argv: list[str], args: argparse.Namespace) -> None:
    if getattr(args, "root_id", None) is not None:
        argv.extend(["--root-id", str(args.root_id)])
    elif getattr(args, "root_path", None) is not None:
        argv.extend(["--root-path", args.root_path])


def _build_add_file_argv(args: argparse.Namespace) -> list[str]:
    argv = _build_common_argv(args)
    _append_root_selector(argv, args)
    if args.force_include:
        argv.append("--force-include")
    if args.disabled:
        argv.append("--disabled")
    argv.append(args.target_path)
    return argv


def _build_add_dir_argv(args: argparse.Namespace) -> list[str]:
    argv = _build_common_argv(args)
    _append_root_selector(argv, args)
    if args.recursive:
        argv.append("--recursive")
    if args.force_include:
        argv.append("--force-include")
    if args.disabled:
        argv.append("--disabled")
    argv.append(args.target_path)
    return argv


def _build_list_argv(args: argparse.Namespace) -> list[str]:
    argv = _build_common_argv(args)
    _append_root_selector(argv, args)
    return argv


def _build_disable_argv(args: argparse.Namespace) -> list[str]:
    argv = _build_common_argv(args)
    argv.extend(["--manual-include-id", str(args.manual_include_id)])
    return argv


def _build_enable_argv(args: argparse.Namespace) -> list[str]:
    argv = _build_common_argv(args)
    argv.extend(["--manual-include-id", str(args.manual_include_id)])
    return argv


if __name__ == "__main__":
    raise SystemExit(main())
