from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.domain.enums import OversizeAction
from backup_projects.repositories.rules_repo import (
    ExcludedPatternRecord,
    ExtensionRuleRecord,
    RulesRepository,
)

_SUPPORTED_EXCLUDE_PATTERN_TYPES = (
    "directory_name",
    "glob",
    "path_substring",
    "regex",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="List and mutate policy rules in SQLite."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "list",
        description="List extension rules and excluded patterns.",
    )

    add_extension_parser = subparsers.add_parser(
        "add-extension",
        description="Create one extension rule row in SQLite.",
    )
    add_extension_parser.add_argument("extension")
    add_extension_parser.add_argument(
        "--oversize-action",
        required=True,
        choices=[action.value for action in OversizeAction],
    )
    add_extension_parser.add_argument("--max-size-bytes", type=int)
    add_extension_parser.add_argument("--disabled", action="store_true")

    update_extension_parser = subparsers.add_parser(
        "update-extension",
        description="Update one extension rule row in SQLite.",
    )
    update_extension_parser.add_argument("extension")
    update_extension_parser.add_argument(
        "--oversize-action",
        choices=[action.value for action in OversizeAction],
    )
    max_size_group = update_extension_parser.add_mutually_exclusive_group()
    max_size_group.add_argument("--max-size-bytes", type=int)
    max_size_group.add_argument("--clear-max-size", action="store_true")
    enabled_group = update_extension_parser.add_mutually_exclusive_group()
    enabled_group.add_argument("--enabled", action="store_true")
    enabled_group.add_argument("--disabled", action="store_true")

    add_exclude_parser = subparsers.add_parser(
        "add-exclude",
        description="Create one excluded pattern row in SQLite.",
    )
    add_exclude_parser.add_argument(
        "--pattern-type",
        required=True,
        choices=_SUPPORTED_EXCLUDE_PATTERN_TYPES,
    )
    add_exclude_parser.add_argument("pattern_value")
    add_exclude_parser.add_argument("--disabled", action="store_true")

    disable_exclude_parser = subparsers.add_parser(
        "disable-exclude",
        description="Disable one excluded pattern row in SQLite.",
    )
    disable_exclude_parser.add_argument("exclude_id", type=int)

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
            repo = RulesRepository(session)
            if args.command == "list":
                extension_rules = tuple(repo.list_extension_rules())
                excluded_patterns = tuple(repo.list_excluded_patterns())
                _print_rules_list(
                    extension_rules=extension_rules,
                    excluded_patterns=excluded_patterns,
                )
            elif args.command == "add-extension":
                _validate_max_size_bytes(args.max_size_bytes)
                now_iso = _now_iso()
                created_rule = repo.create_extension_rule(
                    extension=_normalize_extension(args.extension),
                    enabled=not args.disabled,
                    max_size_bytes=args.max_size_bytes,
                    oversize_action=args.oversize_action,
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                _print_created_extension_rule(created_rule)
            elif args.command == "update-extension":
                updated_rule = _update_extension_rule(repo=repo, args=args)
                _print_updated_extension_rule(updated_rule)
            elif args.command == "add-exclude":
                now_iso = _now_iso()
                created_pattern = repo.create_excluded_pattern(
                    pattern_type=args.pattern_type,
                    pattern_value=args.pattern_value,
                    enabled=not args.disabled,
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                _print_created_excluded_pattern(created_pattern)
            elif args.command == "disable-exclude":
                disabled_pattern = _disable_excluded_pattern(
                    repo=repo,
                    exclude_id=args.exclude_id,
                )
                _print_disabled_excluded_pattern(disabled_pattern)
            else:
                raise ValueError(f"Unsupported rules command: {args.command}")
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except IntegrityError as exc:
        print(_format_integrity_error(args=args, exc=exc), file=sys.stderr)
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


def _update_extension_rule(*, repo: RulesRepository, args) -> ExtensionRuleRecord:
    normalized_extension = _normalize_extension(args.extension)
    existing_rule = repo.get_extension_rule(normalized_extension)
    if existing_rule is None:
        raise LookupError(
            f"Extension rule not found for extension: {normalized_extension}"
        )

    if (
        args.oversize_action is None
        and args.max_size_bytes is None
        and not args.clear_max_size
        and not args.enabled
        and not args.disabled
    ):
        raise ValueError("At least one update field must be provided")

    _validate_max_size_bytes(args.max_size_bytes)
    repo.update_extension_rule(
        existing_rule.id,
        enabled=(
            True if args.enabled else False if args.disabled else existing_rule.enabled
        ),
        max_size_bytes=(
            None
            if args.clear_max_size
            else args.max_size_bytes
            if args.max_size_bytes is not None
            else existing_rule.max_size_bytes
        ),
        oversize_action=args.oversize_action or existing_rule.oversize_action,
        updated_at=_now_iso(),
    )
    refreshed_rule = repo.get_extension_rule(normalized_extension)
    if refreshed_rule is None:
        raise RuntimeError("Failed to reload updated extension rule")
    return refreshed_rule


def _disable_excluded_pattern(
    *,
    repo: RulesRepository,
    exclude_id: int,
) -> ExcludedPatternRecord:
    existing_pattern = repo.get_excluded_pattern(exclude_id)
    if existing_pattern is None:
        raise LookupError(f"Excluded pattern not found for id: {exclude_id}")

    repo.update_excluded_pattern(
        exclude_id,
        enabled=False,
        updated_at=_now_iso(),
    )
    refreshed_pattern = repo.get_excluded_pattern(exclude_id)
    if refreshed_pattern is None:
        raise RuntimeError("Failed to reload disabled excluded pattern")
    return refreshed_pattern


def _print_rules_list(
    *,
    extension_rules: tuple[ExtensionRuleRecord, ...],
    excluded_patterns: tuple[ExcludedPatternRecord, ...],
) -> None:
    print("Extension rules")
    if not extension_rules:
        print("No extension rules found.")
    else:
        for index, rule in enumerate(extension_rules):
            print(f"id: {rule.id}")
            print(f"extension: {rule.extension}")
            print(f"enabled: {str(rule.enabled).lower()}")
            print(f"max-size-bytes: {rule.max_size_bytes}")
            print(f"oversize-action: {rule.oversize_action}")
            if index < len(extension_rules) - 1:
                print()

    print()
    print("Excluded patterns")
    if not excluded_patterns:
        print("No excluded patterns found.")
    else:
        for index, pattern in enumerate(excluded_patterns):
            print(f"id: {pattern.id}")
            print(f"pattern-type: {pattern.pattern_type}")
            print(f"pattern-value: {pattern.pattern_value}")
            print(f"enabled: {str(pattern.enabled).lower()}")
            if index < len(excluded_patterns) - 1:
                print()


def _print_created_extension_rule(rule: ExtensionRuleRecord) -> None:
    print(f"Created extension rule: {rule.id}")
    _print_extension_rule_fields(rule)


def _print_updated_extension_rule(rule: ExtensionRuleRecord) -> None:
    print(f"Updated extension rule: {rule.extension}")
    _print_extension_rule_fields(rule)


def _print_extension_rule_fields(rule: ExtensionRuleRecord) -> None:
    print(f"id: {rule.id}")
    print(f"extension: {rule.extension}")
    print(f"enabled: {str(rule.enabled).lower()}")
    print(f"max-size-bytes: {rule.max_size_bytes}")
    print(f"oversize-action: {rule.oversize_action}")


def _print_created_excluded_pattern(pattern: ExcludedPatternRecord) -> None:
    print(f"Created excluded pattern: {pattern.id}")
    _print_excluded_pattern_fields(pattern)


def _print_disabled_excluded_pattern(pattern: ExcludedPatternRecord) -> None:
    print(f"Disabled excluded pattern: {pattern.id}")
    _print_excluded_pattern_fields(pattern)


def _print_excluded_pattern_fields(pattern: ExcludedPatternRecord) -> None:
    print(f"id: {pattern.id}")
    print(f"pattern-type: {pattern.pattern_type}")
    print(f"pattern-value: {pattern.pattern_value}")
    print(f"enabled: {str(pattern.enabled).lower()}")


def _normalize_extension(extension: str) -> str:
    normalized_extension = extension.strip().lower()
    if normalized_extension.startswith("."):
        normalized_extension = normalized_extension[1:]
    if normalized_extension == "":
        raise ValueError("extension must not be empty")
    return normalized_extension


def _validate_max_size_bytes(max_size_bytes: int | None) -> None:
    if max_size_bytes is not None and max_size_bytes < 0:
        raise ValueError("max_size_bytes must be >= 0")


def _format_integrity_error(*, args, exc: IntegrityError) -> str:
    if args.command == "add-extension":
        return (
            "Extension rule already exists for extension: "
            f"{_normalize_extension(args.extension)}"
        )
    if args.command == "add-exclude":
        return (
            "Excluded pattern already exists: "
            f"{args.pattern_type}:{args.pattern_value}"
        )
    return str(exc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
