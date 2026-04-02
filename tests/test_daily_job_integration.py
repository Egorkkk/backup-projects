from __future__ import annotations

import json
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
from backup_projects.repositories.runs_repo import RunsRepository
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.services.logging_setup import build_run_log_path
from backup_projects.services.post_backup_archive_service import PostBackupArchiveResult
from backup_projects.services.report_delivery_service import ReportDeliveryResult
from backup_projects.services.run_lock import build_run_lock_path


def test_run_daily_job_happy_path_completes_full_flow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")
    show_root_b = tmp_path / "raid_a" / "show-b"
    show_root_b.mkdir(parents=True, exist_ok=True)
    (show_root_b / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    backup_calls = []
    archive_calls = []

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
    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        lambda request: archive_calls.append(request) or None,
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
            assert len(backup_calls) == 1
            assert archive_calls == []
            assert result.backup_result is not None
            assert result.backup_result.restic_result is not None
            assert result.backup_result.restic_result.snapshot_id == "snapshot-daily-1"
            assert result.manifest_result is not None
            assert Path(result.manifest_result.manifest_file_path).is_file()
            assert len(result.targets) == 2
            assert all(target.status == "completed" for target in result.targets)
            assert all(target.backup_result is None for target in result.targets)
            assert {target.root_path.rsplit("/", 1)[-1] for target in result.targets} == {
                "show-a",
                "show-b",
            }
            assert {target.included_count for target in result.targets} == {1}
            assert {target.skipped_count for target in result.targets} == {0}
            assert Path(result.report.json_report_path).is_file()
            assert Path(result.report.text_report_path).is_file()
            assert Path(result.report.html_report_path).is_file()
            assert Path(result.log_file_path).is_file()
            assert result.summary.targets_total == 2
            assert result.summary.targets_succeeded == 2
            assert result.summary.new_count == 1
            assert [event.event_type for event in result.report.report.events] == [
                "roots_discovered",
                "structural_rescan_completed",
                "project_dir_scan_completed",
                "manual_includes_completed",
                "daily_backup_completed",
                "daily_archive_skipped",
                "daily_local_retention_skipped",
                "daily_report_delivery_skipped",
            ]

            active_roots = RootsRepository(session).list_active()
            assert len(active_roots) == 2
            project_dirs = [
                ProjectDirsRepository(session).list_active_by_root(active_root.id)
                for active_root in active_roots
            ]
            assert all(len(root_project_dirs) == 1 for root_project_dirs in project_dirs)
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


def test_run_daily_job_skips_empty_manifest_without_failing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    show_root = tmp_path / "raid_a" / "show-empty"
    show_root.mkdir(parents=True, exist_ok=True)

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)
    archive_calls = []

    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        lambda request: archive_calls.append(request) or None,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 15, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"
            assert len(result.targets) == 1
            assert archive_calls == []

            target = result.targets[0]
            assert target.status == "completed"
            assert target.backup_result is None
            assert target.error is None
            assert target.included_count == 0
            assert target.skipped_count == 0
            assert result.backup_result is not None
            assert result.backup_result.restic_result is None
            assert result.backup_result.message == "Backup skipped: manifest include set is empty"
            assert result.manifest_result is not None
            assert Path(result.manifest_result.manifest_file_path).is_file()

            skipped_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_backup_skipped"
            )
            assert skipped_event.payload is not None
            assert skipped_event.payload["message"] == result.backup_result.message
            assert skipped_event.payload["manifest_file_path"] == result.manifest_result.manifest_file_path

            archive_skipped_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_archive_skipped"
            )
            assert archive_skipped_event.payload == {"reason": "no_snapshot"}

            retention_skipped_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_local_retention_skipped"
            )
            assert retention_skipped_event.payload == {
                "reason": "no_snapshot",
                "keep_last": 1,
            }

            report_target = result.report.report.targets[0]
            assert report_target.status == "completed"
            assert report_target.backup is None
            assert report_target.error is None
            assert result.report.report.backup is None

            log_text = Path(result.log_file_path).read_text(encoding="utf-8")
            assert "Daily backup skipped:" in log_text
            assert "manifest include set is empty" in log_text
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
                if event.event_type == "daily_backup_failed"
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
            assert "Daily backup failed" in log_text
            assert "returncode=10" in log_text
            assert "repo locked" in log_text
    finally:
        engine.dispose()


def test_run_daily_job_runs_archive_and_retention_after_successful_backup_when_enabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, archive_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    call_order: list[str] = []

    def fake_run_backup_from_manifest(request):
        call_order.append("backup")
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-archive-1",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-archive-1",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    def fake_run_post_backup_archive(request):
        call_order.append("archive")
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="completed",
            retention_status="completed",
        )

    monkeypatch.setattr(
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        fake_run_post_backup_archive,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 20, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"
            assert call_order == ["backup", "archive"]
            assert [event.event_type for event in result.report.report.events] == [
                "roots_discovered",
                "structural_rescan_completed",
                "project_dir_scan_completed",
                "manual_includes_completed",
                "daily_backup_completed",
                "daily_archive_started",
                "daily_archive_completed",
                "daily_local_retention_started",
                "daily_local_retention_completed",
                "daily_report_delivery_skipped",
            ]
            report_json = Path(result.report.json_report_path).read_text(encoding="utf-8")
            report_text = Path(result.report.text_report_path).read_text(encoding="utf-8")
            report_html = Path(result.report.html_report_path).read_text(encoding="utf-8")
            assert "daily_local_retention_completed" in report_json
            assert "daily_local_retention_completed" in report_text
            assert "daily_local_retention_completed" in report_html
    finally:
        engine.dispose()


def test_run_daily_job_skips_retention_when_archive_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, archive_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-archive-fail",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-archive-fail",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    archive_failure = ResticCommandFailureError(
        CommandResult(
            argv=("restic", "copy", "snapshot-archive-fail"),
            returncode=3,
            stdout="",
            stderr="remote copy failed",
            duration_seconds=1.0,
        )
    )

    def fake_run_post_backup_archive(request):
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="failed",
            retention_status="skipped",
            archive_error=str(archive_failure),
            archive_exception=archive_failure,
        )

    monkeypatch.setattr(
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        fake_run_post_backup_archive,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 25, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "failed"
            archive_failed_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_archive_failed"
            )
            assert archive_failed_event.payload is not None
            assert archive_failed_event.payload["snapshot_id"] == "snapshot-archive-fail"
            assert archive_failed_event.payload["remote_repository"] == "/mnt/backup/remote-restic-repo"
            assert archive_failed_event.payload["returncode"] == 3
            assert "remote copy failed" in str(archive_failed_event.payload["stderr_excerpt"])

            retention_skipped_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_local_retention_skipped"
            )
            assert retention_skipped_event.payload == {
                "reason": "archive_failed",
                "keep_last": 1,
            }
    finally:
        engine.dispose()


def test_run_daily_job_marks_run_failed_when_local_retention_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, archive_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-retention-fail",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-retention-fail",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    retention_failure = ResticCommandFailureError(
        CommandResult(
            argv=("restic", "forget", "--keep-last", "1", "--group-by", "", "--prune"),
            returncode=3,
            stdout="",
            stderr="local retention failed",
            duration_seconds=1.0,
        )
    )

    def fake_run_post_backup_archive(request):
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="completed",
            retention_status="failed",
            retention_error=str(retention_failure),
            retention_exception=retention_failure,
        )

    monkeypatch.setattr(
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        fake_run_post_backup_archive,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 27, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "failed"
            retention_failed_event = next(
                event
                for event in result.report.report.events
                if event.event_type == "daily_local_retention_failed"
            )
            assert retention_failed_event.payload is not None
            assert retention_failed_event.payload["snapshot_id"] == "snapshot-retention-fail"
            assert retention_failed_event.payload["keep_last"] == 1
            assert retention_failed_event.payload["returncode"] == 3
            assert "local retention failed" in str(
                retention_failed_event.payload["stderr_excerpt"]
            )
    finally:
        engine.dispose()


def test_run_daily_job_with_report_delivery_enabled_delivers_after_report_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, report_delivery_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    call_order: list[str] = []
    original_write_run_report = daily_job_module.write_run_report

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-delivery-1",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-delivery-1",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    def fake_write_run_report(**kwargs):
        call_order.append("report")
        return original_write_run_report(**kwargs)

    def fake_run_report_delivery(request):
        call_order.append("delivery")
        return ReportDeliveryResult(
            status="completed",
            mode=request.mode,
            source_report_path=request.source_report_path,
            destination_path=str(tmp_path / "delivered" / "run-1-report.txt"),
        )

    monkeypatch.setattr(daily_job_module, "run_backup_from_manifest", fake_run_backup_from_manifest)
    monkeypatch.setattr(daily_job_module, "write_run_report", fake_write_run_report)
    monkeypatch.setattr(daily_job_module, "run_report_delivery", fake_run_report_delivery)

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 40, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"
            assert call_order == ["report", "delivery", "report"]
            assert "daily_report_delivery_completed" in {
                event.event_type for event in result.report.report.events
            }
            assert "daily_report_delivery_completed" in Path(
                result.report.json_report_path
            ).read_text(encoding="utf-8")
            assert "daily_report_delivery_completed" in Path(
                result.report.text_report_path
            ).read_text(encoding="utf-8")
            assert "daily_report_delivery_completed" in Path(
                result.report.html_report_path
            ).read_text(encoding="utf-8")

            event_records = RunsRepository(session).list_run_events(result.run.id)
            event_types = [record.event_type for record in event_records]
            assert "daily_report_delivery_started" in event_types
            assert "daily_report_delivery_completed" in event_types
    finally:
        engine.dispose()


def test_run_daily_job_with_report_delivery_disabled_skips_delivery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, report_delivery_enabled=False)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    delivery_calls = []

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-delivery-disabled",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-delivery-disabled",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    monkeypatch.setattr(daily_job_module, "run_backup_from_manifest", fake_run_backup_from_manifest)
    monkeypatch.setattr(
        daily_job_module,
        "run_report_delivery",
        lambda request: delivery_calls.append(request),
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 41, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"
            assert delivery_calls == []

            event_records = RunsRepository(session).list_run_events(result.run.id)
            skipped_event = next(
                record for record in event_records if record.event_type == "daily_report_delivery_skipped"
            )
            assert json.loads(skipped_event.payload_json) == {"reason": "delivery_disabled"}
    finally:
        engine.dispose()


def test_run_daily_job_does_not_attempt_report_delivery_if_report_write_failed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, report_delivery_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    delivery_calls = []

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-delivery-report-fail",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-delivery-report-fail",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    monkeypatch.setattr(daily_job_module, "run_backup_from_manifest", fake_run_backup_from_manifest)
    monkeypatch.setattr(
        daily_job_module,
        "write_run_report",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("report write failed")),
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_report_delivery",
        lambda request: delivery_calls.append(request),
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            try:
                daily_job_module.run_daily_job(
                    session=session,
                    config=config,
                    now=lambda: datetime(2026, 3, 25, 9, 42, tzinfo=timezone.utc),
                )
            except RuntimeError as exc:
                assert str(exc) == "report write failed"
            else:
                raise AssertionError("run_daily_job must raise when report write fails")

            assert delivery_calls == []
    finally:
        engine.dispose()


def test_run_daily_job_records_report_delivery_failure_but_preserves_final_run_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, report_delivery_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    def fake_run_backup_from_manifest(request):
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-delivery-failed",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-delivery-failed",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    monkeypatch.setattr(daily_job_module, "run_backup_from_manifest", fake_run_backup_from_manifest)
    monkeypatch.setattr(
        daily_job_module,
        "run_report_delivery",
        lambda request: ReportDeliveryResult(
            status="failed",
            mode=request.mode,
            source_report_path=request.source_report_path,
            destination_path=str(tmp_path / "delivered" / "run-1-report.txt"),
            error="destination filesystem error",
        ),
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 43, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "completed"

            event_records = RunsRepository(session).list_run_events(result.run.id)
            failed_event = next(
                record for record in event_records if record.event_type == "daily_report_delivery_failed"
            )
            assert json.loads(failed_event.payload_json) == {
                "destination_path": str(tmp_path / "delivered" / "run-1-report.txt"),
                "error": "destination filesystem error",
                "mode": "local_file",
                "source_report_path": result.report.text_report_path,
            }

            log_text = Path(result.log_file_path).read_text(encoding="utf-8")
            assert "Daily report delivery failed: destination filesystem error" in log_text
    finally:
        engine.dispose()


def test_run_daily_job_runs_single_backup_for_partial_target_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path, archive_enabled=True)
    show_root = tmp_path / "raid_a" / "show-a"
    show_root.mkdir(parents=True, exist_ok=True)
    (show_root / "edit.prproj").write_text("project", encoding="utf-8")
    show_root_b = tmp_path / "raid_a" / "show-b"
    show_root_b.mkdir(parents=True, exist_ok=True)
    (show_root_b / "edit.prproj").write_text("project", encoding="utf-8")

    config = load_config(app_path=app_path, rules_path=rules_path)
    _prepare_runtime_dirs(config=config)
    initialize_database(config)

    original_apply_manual_includes_for_root = daily_job_module.apply_manual_includes_for_root
    backup_calls = []
    archive_calls = []

    def fake_apply_manual_includes_for_root(*, session, root_id, applied_at):
        root = RootsRepository(session).get_by_id(root_id)
        assert root is not None
        if root.name == "show-b":
            raise RuntimeError("manual include failed for show-b")
        return original_apply_manual_includes_for_root(
            session=session,
            root_id=root_id,
            applied_at=applied_at,
        )

    def fake_run_backup_from_manifest(request):
        backup_calls.append(request)
        return BackupServiceResult(
            manifest_result=request.manifest_result,
            restic_result=ResticBackupResult(
                manifest_file_path=request.manifest_result.manifest_file_path,
                snapshot_id="snapshot-daily-partial",
                summary_payload={
                    "message_type": "summary",
                    "snapshot_id": "snapshot-daily-partial",
                    "files_new": 1,
                    "files_changed": 0,
                },
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=0.1,
            ),
        )

    def fake_run_post_backup_archive(request):
        archive_calls.append(request)
        return PostBackupArchiveResult(
            snapshot_id=request.snapshot_id,
            remote_repository=request.remote_repository,
            local_retention_keep_last=request.local_retention_keep_last,
            archive_status="completed",
            retention_status="completed",
        )

    monkeypatch.setattr(
        daily_job_module,
        "apply_manual_includes_for_root",
        fake_apply_manual_includes_for_root,
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )
    monkeypatch.setattr(
        daily_job_module,
        "run_post_backup_archive",
        fake_run_post_backup_archive,
    )

    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            result = daily_job_module.run_daily_job(
                session=session,
                config=config,
                now=lambda: datetime(2026, 3, 25, 9, 30, tzinfo=timezone.utc),
            )

            assert isinstance(result, DailyJobFinishedResult)
            assert result.run.status == "failed"
            assert len(backup_calls) == 1
            assert len(archive_calls) == 1
            assert result.backup_result is not None
            assert result.backup_result.restic_result is not None
            assert result.backup_result.restic_result.snapshot_id == "snapshot-daily-partial"
            assert len(result.targets) == 2
            failed_target = next(target for target in result.targets if target.status == "failed")
            completed_target = next(
                target for target in result.targets if target.status == "completed"
            )
            assert failed_target.error == "manual include failed for show-b"
            assert completed_target.included_count == 1
            assert completed_target.skipped_count == 0
            assert result.summary.targets_failed == 1
            assert result.summary.targets_succeeded == 1
    finally:
        engine.dispose()


def _write_config_files(
    tmp_path: Path,
    *,
    archive_enabled: bool = False,
    report_delivery_enabled: bool = False,
) -> tuple[Path, Path]:
    app_path = tmp_path / "app.yaml"
    rules_path = tmp_path / "rules.yaml"
    raid_root_path = (tmp_path / "raid_a").as_posix()
    archive_enabled_value = "true" if archive_enabled else "false"
    archive_remote_repository = (
        '"/mnt/backup/remote-restic-repo"' if archive_enabled else "null"
    )
    archive_remote_password_env_var = (
        '"RESTIC_REMOTE_PASSWORD"' if archive_enabled else "null"
    )
    report_delivery_enabled_value = "true" if report_delivery_enabled else "false"
    report_delivery_mode = '"local_file"' if report_delivery_enabled else "null"
    report_delivery_output_dir = (
        '"runtime/delivered-reports"' if report_delivery_enabled else "null"
    )

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
              archive:
                enabled: {archive_enabled_value}
                remote_repository: {archive_remote_repository}
                remote_password_env_var: {archive_remote_password_env_var}
                local_retention_keep_last: 1
            scheduler:
              mode: "cron"
            report_delivery:
              enabled: {report_delivery_enabled_value}
              mode: {report_delivery_mode}
              output_dir: {report_delivery_output_dir}
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
