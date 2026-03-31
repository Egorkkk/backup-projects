from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from backup_projects.jobs.daily_job import DailyJobLockedResult, DailyJobTargetResult
from backup_projects.services.run_service import RunLifecycleRecord
from backup_projects.services.summary_service import RunCountsSummary


def _make_finished_result(*, status: str, targets: tuple[DailyJobTargetResult, ...]):
    return SimpleNamespace(
        run=RunLifecycleRecord(
            id=41,
            run_type="daily",
            status=status,
            started_at="2026-03-20T10:00:00+00:00",
            trigger_mode="cron",
            finished_at="2026-03-20T10:10:00+00:00",
        ),
        targets=targets,
        summary=RunCountsSummary(
            run_id=41,
            run_type="daily",
            status=status,
            included_count=0,
            skipped_count=0,
            new_count=None,
            changed_count=None,
            targets_total=len(targets),
            targets_succeeded=sum(1 for target in targets if target.status == "completed"),
            targets_failed=sum(1 for target in targets if target.status == "failed"),
            targets=(),
        ),
        report=SimpleNamespace(),
        log_file_path="/tmp/runtime/logs/run-41/run.log",
    )


def test_run_daily_requires_config(capsys) -> None:
    from backup_projects.cli.run_daily import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_run_daily_prints_finished_job_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="completed",
        targets=(
            DailyJobTargetResult(
                root_id=2,
                root_path="/mnt/raid_a/projects/show-b",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.json",
                    summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.summary.txt",
                ),
                backup_result=SimpleNamespace(
                    restic_result=SimpleNamespace(snapshot_id="snapshot-b")
                ),
                error=None,
            ),
            DailyJobTargetResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.manifest.json",
                    summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.summary.txt",
                ),
                backup_result=SimpleNamespace(
                    restic_result=SimpleNamespace(snapshot_id="snapshot-a")
                ),
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

    monkeypatch.setattr(run_daily_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_daily_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_daily_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_daily_module,
        "run_daily_job",
        lambda *, session, config: result,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.index("Daily backup root-id: 2") < captured.out.index(
        "Daily backup root-id: 1"
    )
    assert "manifest-file: /tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt" in captured.out
    assert "snapshot-id: snapshot-b" in captured.out
    assert "manifest-file: /tmp/manifests/daily-20260317T081500Z-root-1.manifest.txt" in captured.out
    assert "snapshot-id: snapshot-a" in captured.out
    assert "Daily run summary" in captured.out
    assert "roots-total: 2" in captured.out
    assert "roots-succeeded: 2" in captured.out
    assert "roots-failed: 0" in captured.out


def test_run_daily_prints_backup_note_for_completed_empty_manifest(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="completed",
        targets=(
            DailyJobTargetResult(
                root_id=3,
                root_path="/mnt/raid_a/projects/show-empty",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/daily-root-3.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/daily-root-3.manifest.json",
                    summary_file_path="/tmp/manifests/daily-root-3.summary.txt",
                ),
                backup_result=SimpleNamespace(restic_result=None),
                error="Backup skipped: manifest include set is empty",
            ),
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_daily_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_daily_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_daily_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_daily_module,
        "run_daily_job",
        lambda *, session, config: result,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "backup-note: Backup skipped: manifest include set is empty" in captured.out
    assert "snapshot-id:" not in captured.out


def test_run_daily_prints_locked_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace()
    locked_result = DailyJobLockedResult(
        run=RunLifecycleRecord(
            id=52,
            run_type="daily",
            status="locked",
            started_at="2026-03-20T10:00:00+00:00",
            trigger_mode="cron",
            finished_at="2026-03-20T10:00:02+00:00",
        ),
        lock_path="/tmp/runtime/locks/run.lock",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_daily_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_daily_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_daily_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_daily_module,
        "run_daily_job",
        lambda *, session, config: locked_result,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Daily run locked for run-id: 52" in captured.out
    assert "lock-file: /tmp/runtime/locks/run.lock" in captured.out


def test_run_daily_maps_failed_job_result_to_exit_code_1(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="failed",
        targets=(
            DailyJobTargetResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="failed",
                manifest_result=None,
                backup_result=None,
                error="backup failed for root 1",
            ),
            DailyJobTargetResult(
                root_id=2,
                root_path="/mnt/raid_a/projects/show-b",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.json",
                    summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.summary.txt",
                ),
                backup_result=SimpleNamespace(
                    restic_result=SimpleNamespace(snapshot_id="snapshot-b")
                ),
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

    monkeypatch.setattr(run_daily_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_daily_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_daily_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_daily_module,
        "run_daily_job",
        lambda *, session, config: result,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "backup failed for root 1" in captured.err
    assert "Daily backup root-id: 2" in captured.out
    assert "roots-total: 2" in captured.out
    assert "roots-succeeded: 1" in captured.out
    assert "roots-failed: 1" in captured.out


def test_run_daily_config_error_returns_exit_code_2(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module
    from backup_projects.config import ConfigValidationError

    monkeypatch.setattr(
        run_daily_module,
        "load_config",
        lambda app_path, rules_path: (_ for _ in ()).throw(
            ConfigValidationError("bad config")
        ),
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.strip() == "bad config"
