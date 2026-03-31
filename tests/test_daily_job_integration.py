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
from backup_projects.adapters.process.command_runner import CommandResult
from backup_projects.adapters.process.restic_runner import ResticCommandFailureError
from backup_projects.adapters.filesystem.file_lock import acquire_file_lock
from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.config import load_config
from backup_projects.jobs import daily_job as daily_job_module
from backup_projects.jobs.daily_job import DailyJobFinishedResult, DailyJobLockedResult
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.services.logging_setup import build_run_log_path
from backup_projects.services.run_lock import build_run_lock_path


def test_run_daily_job_happy_path_completes_full_flow(
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

    def fake_run_backup_from_manifest(request):
        manifest_path = Path(request.manifest_result.manifest_file_path)
        assert manifest_path.is_file()
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-daily-1",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-daily-1",
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
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 0, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"
            assert len(result.targets) == 1
            assert result.targets[0].status == "completed"
            assert result.targets[0].backup_result is not None
            assert result.targets[0].backup_result.restic_result.snapshot_id == "snapshot-daily-1"
            assert Path(result.targets[0].manifest_result.manifest_file_path).is_file()
            assert Path(result.report.json_report_path).is_file()
            assert Path(result.report.text_report_path).is_file()
            assert Path(result.report.html_report_path).is_file()
            assert Path(result.log_file_path).is_file()
            assert result.summary.targets_total == 1
            assert result.summary.targets_succeeded == 1
            assert [event.event_type for event in result.report.report.events] == [
                "roots_discovered",
                "structural_rescan_completed",
                "project_dir_scan_completed",
                "manual_includes_completed",
                "daily_root_completed",
            ]

            active_roots = RootsRepository(session).list_active()
            assert len(active_roots) == 1
            project_dirs = ProjectDirsRepository(session).list_active_by_root(active_roots[0].id)
            assert len(project_dirs) == 1
    finally:
        engine.dispose()


def test_run_daily_job_locked_result_creates_no_report_or_log(tmp_path: Path) -> None:
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
                result = daily_job_module.run_daily_job(
                    session=session,
                    config=config,
                    now=lambda: datetime(2026, 3, 25, 9, 5, tzinfo=timezone.utc),
                )

                assert isinstance(result, DailyJobLockedResult)
                assert result.run.status == "locked"
                assert not (tmp_path / "runtime" / "reports" / f"run-{result.run.id}").exists()
                assert not build_run_log_path(
                    logs_dir=tmp_path / "runtime" / "logs",
                    run_id=result.run.id,
                ).exists()
    finally:
        engine.dispose()


def test_run_daily_job_surfaces_restic_failure_diagnostics(
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

    failure = ResticCommandFailureError(
        CommandResult(
            argv=(
                "restic",
                "backup",
                "--json",
                "--files-from-verbatim",
                "/tmp/final.manifest.txt",
            ),
            returncode=10,
            stdout="status line\nsecondary detail\n" * 50,
            stderr="repo locked\ntry again later\n" * 50,
            duration_seconds=1.5,
        )
    )

    def fake_run_backup_from_manifest(request):
        raise failure

    monkeypatch.setattr(
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 0, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "failed"
            assert len(result.targets) == 1
            target = result.targets[0]
            assert target.status == "failed"
            assert target.backup_result is None
            assert target.error is not None
            assert "returncode=10" in target.error
            assert "stderr='repo locked" in target.error
            assert "stdout='status line" in target.error
            assert "<truncated>" in target.error

            failure_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_root_failed"
            )
            assert failure_event.payload is not None
            assert failure_event.payload["error"] == str(failure)
            assert failure_event.payload["returncode"] == 10
            assert failure_event.payload["argv"] == [
                "restic",
                "backup",
                "--json",
                "--files-from-verbatim",
                "/tmp/final.manifest.txt",
            ]
            assert "repo locked" in str(failure_event.payload["stderr_excerpt"])
            assert "status line" in str(failure_event.payload["stdout_excerpt"])
            assert "<truncated>" in str(failure_event.payload["stderr_excerpt"])
            assert "<truncated>" in str(failure_event.payload["stdout_excerpt"])

            log_text = Path(result.log_file_path).read_text(encoding="utf-8")
            assert "Daily backup failed for root" in log_text
            assert "returncode=10" in log_text
            assert "repo locked" in log_text
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
              sqlite_path: "runtime/db/jobs.sqlite3"
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
