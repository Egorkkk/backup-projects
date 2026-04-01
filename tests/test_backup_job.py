from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

from backup_projects.adapters.db.bootstrap import initialize_database
from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.adapters.filesystem.file_lock import acquire_file_lock
from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.config import load_config
from backup_projects.jobs import backup_job as backup_job_module
from backup_projects.jobs.backup_job import (
    BackupJobFinishedResult,
    BackupJobLockedResult,
    run_backup_job,
)
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.services.logging_setup import build_run_log_path
from backup_projects.services.run_lock import build_run_lock_path
from backup_projects.services.structural_scan_service import scan_root_structure
from backup_projects.services.structural_scan_sync_service import sync_structural_scan_result


def test_run_backup_job_happy_path_completes_and_writes_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)

    backup_calls = []

    def fake_run_backup_from_manifest(request):
        backup_calls.append(request)
        manifest_path = Path(request.manifest_result.manifest_file_path)
        assert manifest_path.is_file()
        manifest_text = manifest_path.read_text(encoding="utf-8")
        assert "show-a/edit.prproj" in manifest_text
        assert "show-b/edit.prproj" in manifest_text
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-backup-1",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-backup-1",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    monkeypatch.setattr(
        backup_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )

    try:
        with session_scope(session_factory) as session:
            root = RootsRepository(session).create(
                raid_name="raid_a",
                name="show-a",
                path=show_root.as_posix(),
                device_id=None,
                inode=None,
                mtime_ns=None,
                ctime_ns=None,
                first_seen_at="2026-03-25T10:55:00+00:00",
                last_seen_at="2026-03-25T10:55:00+00:00",
            )
            scan_result = scan_root_structure(
                root_path=root.path,
                allowed_extensions=config.rules_config.allowed_extensions,
            )
            sync_structural_scan_result(
                session=session,
                root_id=root.id,
                scan_result=scan_result,
                synced_at="2026-03-25T10:56:00+00:00",
            )
            other_root = RootsRepository(session).create(
                raid_name="raid_a",
                name="show-b",
                path=(tmp_path / "raid_a" / "show-b").as_posix(),
                device_id=None,
                inode=None,
                mtime_ns=None,
                ctime_ns=None,
                first_seen_at="2026-03-25T10:55:00+00:00",
                last_seen_at="2026-03-25T10:55:00+00:00",
            )
            (tmp_path / "raid_a" / "show-b").mkdir(parents=True, exist_ok=True)
            ((tmp_path / "raid_a" / "show-b") / "edit.prproj").write_text(
                "project",
                encoding="utf-8",
            )
            other_scan_result = scan_root_structure(
                root_path=other_root.path,
                allowed_extensions=config.rules_config.allowed_extensions,
            )
            sync_structural_scan_result(
                session=session,
                root_id=other_root.id,
                scan_result=other_scan_result,
                synced_at="2026-03-25T10:56:30+00:00",
            )

            result = run_backup_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 11, 0, tzinfo=timezone.utc),
            )

            assert isinstance(result, BackupJobFinishedResult)
            assert result.run.status == "completed"
            assert len(backup_calls) == 1
            assert result.backup_result is not None
            assert result.backup_result.restic_result is not None
            assert result.backup_result.restic_result.snapshot_id == "snapshot-backup-1"
            assert result.manifest_result is not None
            assert Path(result.manifest_result.manifest_file_path).is_file()
            assert len(result.roots) == 2
            assert all(root_result.status == "completed" for root_result in result.roots)
            assert all(root_result.backup_result is None for root_result in result.roots)
            assert {root_result.included_count for root_result in result.roots} == {1}
            assert Path(result.report.json_report_path).is_file()
            assert Path(result.report.text_report_path).is_file()
            assert Path(result.report.html_report_path).is_file()
            assert Path(result.log_file_path).is_file()
            assert result.summary.targets_total == 2
    finally:
        engine.dispose()


def test_run_backup_job_locked_result_creates_no_report_or_log(tmp_path: Path) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    lock_path = build_run_lock_path(locks_dir=tmp_path / "runtime" / "locks")
    try:
        with acquire_file_lock(lock_path):
            with session_scope(session_factory) as session:
                result = run_backup_job(
                    session=session,
                    config=config,
                    now=lambda: datetime(2026, 3, 25, 11, 5, tzinfo=timezone.utc),
                )

                assert isinstance(result, BackupJobLockedResult)
                assert result.run.status == "locked"
                assert not (tmp_path / "runtime" / "reports" / f"run-{result.run.id}").exists()
                assert not build_run_log_path(
                    logs_dir=tmp_path / "runtime" / "logs",
                    run_id=result.run.id,
                ).exists()
    finally:
        engine.dispose()


def test_run_backup_job_zero_active_roots_returns_completed_result(tmp_path: Path) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = run_backup_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 11, 10, tzinfo=timezone.utc),
            )

            assert isinstance(result, BackupJobFinishedResult)
            assert result.run.status == "completed"
            assert result.manifest_result is None
            assert result.backup_result is None
            assert result.roots == ()
            assert result.summary.targets_total == 0
            assert Path(result.report.json_report_path).is_file()
            assert Path(result.log_file_path).is_file()
    finally:
        engine.dispose()


def test_run_backup_job_partial_failure_still_runs_single_combined_backup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    show_root_a = tmp_path / "raid_a" / "show-a"
    show_root_a.mkdir(parents=True, exist_ok=True)
    (show_root_a / "edit.prproj").write_text("project", encoding="utf-8")
    show_root_b = tmp_path / "raid_a" / "show-b"
    show_root_b.mkdir(parents=True, exist_ok=True)
    (show_root_b / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    backup_calls = []

    original_build_multi_root_dry_run_manifest = backup_job_module.build_multi_root_dry_run_manifest

    def fake_build_multi_root_dry_run_manifest(*, session, root_ids):
        plan = original_build_multi_root_dry_run_manifest(session=session, root_ids=root_ids)
        adjusted_root_plans = []
        for root_plan in plan.root_plans:
            root = RootsRepository(session).get_by_id(root_plan.root_id)
            assert root is not None
            if root.name == "show-b":
                adjusted_root_plans.append(
                    type(root_plan)(
                        root_id=root_plan.root_id,
                        status="failed",
                        error="manifest planning failed for show-b",
                    )
                )
                continue
            adjusted_root_plans.append(root_plan)
        return type(plan)(
            root_plans=tuple(adjusted_root_plans),
            built_manifest=adjusted_root_plans[0].built_manifest,
        )

    def fake_run_backup_from_manifest(request):
        backup_calls.append(request)
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-backup-partial",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-backup-partial",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    monkeypatch.setattr(
        backup_job_module,
        "build_multi_root_dry_run_manifest",
        fake_build_multi_root_dry_run_manifest,
    )
    monkeypatch.setattr(
        backup_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )

    try:
        with session_scope(session_factory) as session:
            for show_root in (show_root_a, show_root_b):
                root = RootsRepository(session).create(
                    raid_name="raid_a",
                    name=show_root.name,
                    path=show_root.as_posix(),
                    device_id=None,
                    inode=None,
                    mtime_ns=None,
                    ctime_ns=None,
                    first_seen_at="2026-03-25T10:55:00+00:00",
                    last_seen_at="2026-03-25T10:55:00+00:00",
                )
                scan_result = scan_root_structure(
                    root_path=root.path,
                    allowed_extensions=config.rules_config.allowed_extensions,
                )
                sync_structural_scan_result(
                    session=session,
                    root_id=root.id,
                    scan_result=scan_result,
                    synced_at="2026-03-25T10:56:00+00:00",
                )

            result = run_backup_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 11, 15, tzinfo=timezone.utc),
            )

            assert isinstance(result, BackupJobFinishedResult)
            assert result.run.status == "failed"
            assert len(backup_calls) == 1
            assert result.backup_result is not None
            assert result.backup_result.restic_result is not None
            assert result.backup_result.restic_result.snapshot_id == "snapshot-backup-partial"
            failed_root = next(root for root in result.roots if root.status == "failed")
            completed_root = next(root for root in result.roots if root.status == "completed")
            assert failed_root.error == "manifest planning failed for show-b"
            assert completed_root.included_count == 1
            assert result.summary.targets_failed == 1
            assert result.summary.targets_succeeded == 1
    finally:
        engine.dispose()


def _write_config_files(tmp_path: Path) -> tuple[Path, Path]:
    app_path = tmp_path / "app.yaml"
    rules_path = tmp_path / "rules.yaml"
    raid_root_path = (tmp_path / "raid_a").as_posix()

    app_path.write_text(
        dedent(
            f"""
            app:
              name: "backup-projects"
              env: "test"
              log_level: "INFO"
            raid_roots:
              - name: "raid_a"
                path: "{raid_root_path}"
                enabled: true
            runtime:
              logs_dir: "runtime/logs"
              manifests_dir: "runtime/manifests"
              reports_dir: "runtime/reports"
              db_dir: "runtime/db"
              locks_dir: "runtime/locks"
            web:
              host: "127.0.0.1"
              port: 8080
              debug: false
            db:
              driver: "sqlite"
              sqlite_path: "runtime/db/backup-job.sqlite3"
            restic:
              binary: "restic"
              repository: "/mnt/backup/restic-repo"
              password_env_var: "RESTIC_PASSWORD"
              timeout_seconds: 7200
            scheduler:
              mode: "cron"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    rules_path.write_text(
        dedent(
            """
            allowed_extensions:
              - "prproj"
              - "aaf"
            size_limits:
              default_max_size_bytes: null
              by_extension:
                aaf: 104857600
            oversize:
              default_action: "warn"
              aaf_action: "skip"
              log_skipped: true
            exclude_patterns:
              directory_names: []
              glob_patterns: []
              path_substrings: []
            unknown_extensions:
              action: "collect_and_skip"
              store_in_registry: true
              log_warning: true
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return app_path, rules_path


def _prepare_runtime_dirs(*, config) -> None:
    runtime_root = config.app_path.parent / "runtime"
    for relative in ("logs", "manifests", "reports", "locks"):
        (runtime_root / relative).mkdir(parents=True, exist_ok=True)
