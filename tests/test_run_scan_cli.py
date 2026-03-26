from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.jobs.scan_job import (
    ScanJobLockedResult,
    ScanJobRootResult,
)
from backup_projects.services.run_service import RunLifecycleRecord
from backup_projects.services.summary_service import RunCountsSummary


def _make_finished_result(*, status: str, roots: tuple[ScanJobRootResult, ...]):
    return SimpleNamespace(
        run=RunLifecycleRecord(
            id=61,
            run_type="scan",
            status=status,
            started_at="2026-03-25T10:00:00+00:00",
            trigger_mode="manual",
            finished_at="2026-03-25T10:05:00+00:00",
        ),
        roots=roots,
        summary=RunCountsSummary(
            run_id=61,
            run_type="scan",
            status=status,
            included_count=0,
            skipped_count=0,
            new_count=None,
            changed_count=None,
            targets_total=len(roots),
            targets_succeeded=sum(1 for root in roots if root.status == "completed"),
            targets_failed=sum(1 for root in roots if root.status == "failed"),
            targets=(),
        ),
        report=SimpleNamespace(),
        log_file_path="/tmp/runtime/logs/run-61/run.log",
    )


def test_run_scan_requires_config(capsys) -> None:
    from backup_projects.cli.run_scan import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_run_scan_prints_finished_job_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_scan as run_scan_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="completed",
        roots=(
            ScanJobRootResult(
                root_id=2,
                root_path="/mnt/raid_a/projects/show-b",
                status="completed",
                structural_rescan_performed=True,
                scanned_project_dir_count=3,
                new_file_count=0,
                changed_file_count=1,
                reactivated_file_count=0,
                missing_file_count=0,
                processed_manual_include_count=0,
                applied_manual_include_count=0,
                error=None,
            ),
            ScanJobRootResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="completed",
                structural_rescan_performed=False,
                scanned_project_dir_count=1,
                new_file_count=0,
                changed_file_count=0,
                reactivated_file_count=0,
                missing_file_count=0,
                processed_manual_include_count=0,
                applied_manual_include_count=0,
                error=None,
            ),
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_scan_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_scan_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_scan_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_scan_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(run_scan_module, "run_scan_job", lambda *, session, config: result)

    exit_code = run_scan_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.index("Scan root-id: 2") < captured.out.index("Scan root-id: 1")
    assert "structural-rescan-performed: true" in captured.out
    assert "project-dirs-scanned: 3" in captured.out
    assert "files-changed: 1" in captured.out
    assert "Scan run summary" in captured.out
    assert "roots-total: 2" in captured.out
    assert "roots-succeeded: 2" in captured.out
    assert "roots-failed: 0" in captured.out


def test_run_scan_prints_locked_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_scan as run_scan_module

    fake_config = SimpleNamespace()
    locked_result = ScanJobLockedResult(
        run=RunLifecycleRecord(
            id=62,
            run_type="scan",
            status="locked",
            started_at="2026-03-25T10:00:00+00:00",
            trigger_mode="manual",
            finished_at="2026-03-25T10:00:02+00:00",
        ),
        lock_path="/tmp/runtime/locks/run.lock",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_scan_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_scan_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_scan_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_scan_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_scan_module,
        "run_scan_job",
        lambda *, session, config: locked_result,
    )

    exit_code = run_scan_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Scan run locked for run-id: 62" in captured.out
    assert "lock-file: /tmp/runtime/locks/run.lock" in captured.out


def test_run_scan_maps_failed_job_result_to_exit_code_1(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_scan as run_scan_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="failed",
        roots=(
            ScanJobRootResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="failed",
                structural_rescan_performed=False,
                scanned_project_dir_count=0,
                new_file_count=0,
                changed_file_count=0,
                reactivated_file_count=0,
                missing_file_count=0,
                processed_manual_include_count=0,
                applied_manual_include_count=0,
                error="scan failed for root 1",
            ),
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_scan_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_scan_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_scan_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_scan_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(run_scan_module, "run_scan_job", lambda *, session, config: result)

    exit_code = run_scan_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "scan failed for root 1"
    assert "Scan run summary" in captured.out
    assert "roots-failed: 1" in captured.out
