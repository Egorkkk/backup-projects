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
from backup_projects.jobs.daily_job import (
    DailyJobFinishedResult,
    DailyJobLockedResult,
    run_daily_job,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "run-daily",
        description="Run the daily backup pipeline for all active roots.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the daily backup pipeline for all active roots."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            result = run_daily_job(
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

    if isinstance(result, DailyJobLockedResult):
        _print_locked_run(result)
        return 0

    _print_finished_result(result)
    return 0 if result.run.status == "completed" else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


def _print_finished_result(result: DailyJobFinishedResult) -> None:
    _print_run_artifacts(
        manifest_result=result.manifest_result,
        snapshot_id=(
            result.backup_result.restic_result.snapshot_id
            if result.backup_result is not None
            and result.backup_result.restic_result is not None
            else None
        ),
        backup_note=(
            result.backup_result.message
            if result.backup_result is not None
            else None
        ),
    )

    for target in result.targets:
        _print_root_result(
            root_id=target.root_id,
            root_path=target.root_path,
            status=target.status,
            included_count=target.included_count,
            skipped_count=target.skipped_count,
        )

        if target.error is not None:
            print(target.error, file=sys.stderr)

    _print_summary(
        roots_total=result.summary.targets_total,
        roots_succeeded=result.summary.targets_succeeded,
        roots_failed=result.summary.targets_failed,
    )


def _print_run_artifacts(
    *,
    manifest_result,
    snapshot_id: str | None,
    backup_note: str | None = None,
) -> None:
    if manifest_result is None:
        return
    print("Daily backup run")
    print(f"manifest-file: {manifest_result.manifest_file_path}")
    print(f"json-manifest-file: {manifest_result.json_manifest_file_path}")
    print(f"summary-file: {manifest_result.summary_file_path}")
    if snapshot_id is not None:
        print(f"snapshot-id: {snapshot_id}")
    elif backup_note is not None:
        print(f"backup-note: {backup_note}")
    print()


def _print_root_result(
    *,
    root_id: int,
    root_path: str,
    status: str,
    included_count: int,
    skipped_count: int,
) -> None:
    print(f"Daily backup root-id: {root_id}")
    print(f"root-path: {root_path}")
    print(f"status: {status}")
    print(f"included-count: {included_count}")
    print(f"skipped-count: {skipped_count}")
    print()


def _print_summary(
    *,
    roots_total: int,
    roots_succeeded: int,
    roots_failed: int,
) -> None:
    print("Daily run summary")
    print(f"roots-total: {roots_total}")
    print(f"roots-succeeded: {roots_succeeded}")
    print(f"roots-failed: {roots_failed}")


def _print_locked_run(lock_result: DailyJobLockedResult) -> None:
    print(f"Daily run locked for run-id: {lock_result.run.id}")
    print(f"lock-file: {lock_result.lock_path}")


if __name__ == "__main__":
    raise SystemExit(main())
