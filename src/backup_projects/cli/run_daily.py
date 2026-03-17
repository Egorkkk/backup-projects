from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.backup_service import (
    BackupServiceRequest,
    run_backup_from_manifest,
)
from backup_projects.services.dry_run_service import build_root_dry_run_manifest
from backup_projects.services.manifest_builder import write_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the daily backup pipeline for all active roots."
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
            roots = tuple(RootsRepository(session).list_active())
            if not roots:
                print("No active roots found.")
                return 0

            return _run_daily_for_roots(
                session=session,
                roots=roots,
                config=config,
            )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2
    finally:
        engine = locals().get("engine")
        if engine is not None:
            engine.dispose()


def _run_daily_for_roots(*, session, roots, config) -> int:
    manifests_dir = Path(config.app_config.runtime.manifests_dir)
    run_timestamp = _current_run_timestamp()
    roots_succeeded = 0
    roots_failed = 0

    for root in roots:
        try:
            built_manifest = build_root_dry_run_manifest(
                session=session,
                root_id=root.id,
            )
            manifest_result = write_manifest(
                built_manifest=built_manifest,
                output_dir=manifests_dir,
                artifact_stem=_build_artifact_stem(
                    root_id=root.id,
                    run_timestamp=run_timestamp,
                ),
            )
            backup_service_result = run_backup_from_manifest(
                BackupServiceRequest(
                    manifest_result=manifest_result,
                    restic_binary=config.app_config.restic.binary,
                    restic_repository=config.app_config.restic.repository,
                    restic_password_env_var=config.app_config.restic.password_env_var,
                    restic_timeout_seconds=config.app_config.restic.timeout_seconds,
                )
            )
        except Exception as exc:
            roots_failed += 1
            print(str(exc), file=sys.stderr)
            continue

        roots_succeeded += 1
        _print_root_success(
            root_id=root.id,
            root_path=root.path,
            manifest_result=manifest_result,
            snapshot_id=backup_service_result.restic_result.snapshot_id,
        )

    _print_summary(
        roots_total=len(roots),
        roots_succeeded=roots_succeeded,
        roots_failed=roots_failed,
    )
    return 0 if roots_failed == 0 else 1


def _current_run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _build_artifact_stem(*, root_id: int, run_timestamp: str) -> str:
    return f"daily-{run_timestamp}-root-{root_id}"


def _print_root_success(
    *,
    root_id: int,
    root_path: str,
    manifest_result,
    snapshot_id: str,
) -> None:
    print(f"Daily backup root-id: {root_id}")
    print(f"root-path: {root_path}")
    print(f"manifest-file: {manifest_result.manifest_file_path}")
    print(f"json-manifest-file: {manifest_result.json_manifest_file_path}")
    print(f"summary-file: {manifest_result.summary_file_path}")
    print(f"snapshot-id: {snapshot_id}")
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


if __name__ == "__main__":
    raise SystemExit(main())
