from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.jobs.backup_job import (
    BackupJobLockedResult,
    BackupJobRootResult,
)
from backup_projects.services.run_service import RunLifecycleRecord
from backup_projects.services.summary_service import RunCountsSummary


def _make_finished_result(*, status: str, roots: tuple[BackupJobRootResult, ...]):
    return SimpleNamespace(
        run=RunLifecycleRecord(
            id=71,
            run_type="backup",
            status=status,
            started_at="2026-03-25T11:00:00+00:00",
            trigger_mode="manual",
            finished_at="2026-03-25T11:05:00+00:00",
        ),
        roots=roots,
        summary=RunCountsSummary(
            run_id=71,
            run_type="backup",
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
        log_file_path="/tmp/runtime/logs/run-71/run.log",
    )


def test_run_backup_requires_config(capsys) -> None:
    from backup_projects.cli.run_backup import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_run_backup_prints_finished_job_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_backup as run_backup_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="completed",
        roots=(
            BackupJobRootResult(
                root_id=2,
                root_path="/mnt/raid_a/projects/show-b",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/backup-root-2.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/backup-root-2.manifest.json",
                    summary_file_path="/tmp/manifests/backup-root-2.summary.txt",
                ),
                backup_result=SimpleNamespace(
                    restic_result=SimpleNamespace(snapshot_id="snapshot-b")
                ),
                error=None,
            ),
            BackupJobRootResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/backup-root-1.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/backup-root-1.manifest.json",
                    summary_file_path="/tmp/manifests/backup-root-1.summary.txt",
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

    monkeypatch.setattr(run_backup_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_backup_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_backup_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_backup_module,
        "run_backup_job",
        lambda *, session, config: result,
    )

    exit_code = run_backup_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.index("Backup root-id: 2") < captured.out.index("Backup root-id: 1")
    assert "manifest-file: /tmp/manifests/backup-root-2.manifest.txt" in captured.out
    assert "snapshot-id: snapshot-b" in captured.out
    assert "Backup run summary" in captured.out
    assert "roots-total: 2" in captured.out
    assert "roots-succeeded: 2" in captured.out
    assert "roots-failed: 0" in captured.out


def test_run_backup_prints_backup_note_for_completed_empty_manifest(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_backup as run_backup_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="completed",
        roots=(
            BackupJobRootResult(
                root_id=3,
                root_path="/mnt/raid_a/projects/show-empty",
                status="completed",
                manifest_result=SimpleNamespace(
                    manifest_file_path="/tmp/manifests/backup-root-3.manifest.txt",
                    json_manifest_file_path="/tmp/manifests/backup-root-3.manifest.json",
                    summary_file_path="/tmp/manifests/backup-root-3.summary.txt",
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

    monkeypatch.setattr(run_backup_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_backup_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_backup_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_backup_module,
        "run_backup_job",
        lambda *, session, config: result,
    )

    exit_code = run_backup_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "backup-note: Backup skipped: manifest include set is empty" in captured.out
    assert "snapshot-id:" not in captured.out


def test_run_backup_prints_locked_result(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_backup as run_backup_module

    fake_config = SimpleNamespace()
    locked_result = BackupJobLockedResult(
        run=RunLifecycleRecord(
            id=72,
            run_type="backup",
            status="locked",
            started_at="2026-03-25T11:00:00+00:00",
            trigger_mode="manual",
            finished_at="2026-03-25T11:00:02+00:00",
        ),
        lock_path="/tmp/runtime/locks/run.lock",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_backup_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_backup_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_backup_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_backup_module,
        "run_backup_job",
        lambda *, session, config: locked_result,
    )

    exit_code = run_backup_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Backup run locked for run-id: 72" in captured.out
    assert "lock-file: /tmp/runtime/locks/run.lock" in captured.out


def test_run_backup_maps_failed_job_result_to_exit_code_1(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_backup as run_backup_module

    fake_config = SimpleNamespace()
    result = _make_finished_result(
        status="failed",
        roots=(
            BackupJobRootResult(
                root_id=1,
                root_path="/mnt/raid_a/projects/show-a",
                status="failed",
                manifest_result=None,
                backup_result=None,
                error="backup failed for root 1",
            ),
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(run_backup_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(run_backup_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(run_backup_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(run_backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        run_backup_module,
        "run_backup_job",
        lambda *, session, config: result,
    )

    exit_code = run_backup_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "backup failed for root 1"
    assert "Backup run summary" in captured.out
    assert "roots-failed: 1" in captured.out
