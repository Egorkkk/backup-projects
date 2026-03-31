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
from backup_projects.jobs.backup_job import (
    BackupJobFinishedResult,
    BackupJobLockedResult,
    run_backup_job,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the backup pipeline for all active roots."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            result = run_backup_job(
                session=session,
                config=config,
            )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        engine = locals().get("engine")
        if engine is not None:
            engine.dispose()

    if isinstance(result, BackupJobLockedResult):
        _print_locked_run(result)
        return 0

    _print_finished_result(result)
    return 0 if result.run.status == "completed" else 1


def _print_finished_result(result: BackupJobFinishedResult) -> None:
    for root in result.roots:
        if root.status == "completed":
            _print_root_success(root)
            continue

        if root.error is not None:
            print(root.error, file=sys.stderr)

    _print_summary(
        roots_total=result.summary.targets_total,
        roots_succeeded=result.summary.targets_succeeded,
        roots_failed=result.summary.targets_failed,
    )


def _print_root_success(root) -> None:
    print(f"Backup root-id: {root.root_id}")
    print(f"root-path: {root.root_path}")
    print(f"manifest-file: {root.manifest_result.manifest_file_path}")
    print(f"json-manifest-file: {root.manifest_result.json_manifest_file_path}")
    print(f"summary-file: {root.manifest_result.summary_file_path}")
    if root.backup_result is not None and root.backup_result.restic_result is not None:
        print(f"snapshot-id: {root.backup_result.restic_result.snapshot_id}")
    elif root.error is not None:
        print(f"backup-note: {root.error}")
    print()


def _print_summary(
    *,
    roots_total: int,
    roots_succeeded: int,
    roots_failed: int,
) -> None:
    print("Backup run summary")
    print(f"roots-total: {roots_total}")
    print(f"roots-succeeded: {roots_succeeded}")
    print(f"roots-failed: {roots_failed}")


def _print_locked_run(lock_result: BackupJobLockedResult) -> None:
    print(f"Backup run locked for run-id: {lock_result.run.id}")
    print(f"lock-file: {lock_result.lock_path}")


if __name__ == "__main__":
    raise SystemExit(main())
