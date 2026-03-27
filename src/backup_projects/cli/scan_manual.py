from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.services.manual_include_scan_service import (
    ManualIncludeScanResult,
    apply_manual_includes_for_root,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "scan-manual",
        description="Apply manual includes for one known root.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.add_argument("--root-id", required=True, type=int)
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply manual includes for one known root."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.add_argument("--root-id", required=True, type=int)
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            result = apply_manual_includes_for_root(
                session=session,
                root_id=args.root_id,
                applied_at=datetime.now(timezone.utc).isoformat(),
            )
        _print_summary(result)
    except ConfigError as exc:
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


def _print_summary(result: ManualIncludeScanResult) -> None:
    print(f"Scanned target: {result.root_path}")
    print(f"root-id: {result.root_id}")
    print(f"manual-includes-processed: {result.processed_include_count}")
    print(f"manual-includes-applied: {result.applied_include_count}")
    print(f"manual-includes-skipped-disabled: {result.skipped_disabled_include_count}")
    print(f"manual-includes-missing-target: {result.missing_target_include_count}")
    print(f"manual-includes-invalid: {result.invalid_include_count}")
    print(f"manual-includes-type-mismatch: {result.type_mismatch_include_count}")
    print(f"manual-includes-errors: {result.error_include_count}")
    print(f"matched-files: {result.matched_file_count}")
    print(f"skipped-files: {result.skipped_file_count}")
    print(f"project-dirs-created: {result.created_project_dir_count}")
    print(f"project-dirs-updated: {result.updated_project_dir_count}")
    print(f"project-dirs-reactivated: {result.reactivated_project_dir_count}")
    print(f"project-files-created: {result.created_project_file_count}")
    print(f"project-files-updated: {result.updated_project_file_count}")
    print(f"project-files-reactivated: {result.reactivated_project_file_count}")
    print(f"project-files-unchanged: {result.unchanged_project_file_count}")


if __name__ == "__main__":
    raise SystemExit(main())
