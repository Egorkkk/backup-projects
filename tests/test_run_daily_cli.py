from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain import ManifestResult
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.services.manifest_builder import BuiltManifest


def test_run_daily_requires_config(capsys) -> None:
    from backup_projects.cli.run_daily import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_build_artifact_stem_includes_timestamp_and_root_id() -> None:
    from backup_projects.cli.run_daily import _build_artifact_stem

    assert (
        _build_artifact_stem(root_id=7, run_timestamp="20260317T081500Z")
        == "daily-20260317T081500Z-root-7"
    )


def test_run_daily_success_flow_for_multiple_roots(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace(
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(manifests_dir="/tmp/manifests"),
            restic=SimpleNamespace(
                binary="restic",
                repository="/mnt/backup/repo",
                password_env_var="RESTIC_PASSWORD",
                timeout_seconds=7200,
            ),
        )
    )
    calls: list[tuple] = []
    roots = (
        SimpleNamespace(id=2, path="/mnt/raid_a/projects/show-b"),
        SimpleNamespace(id=1, path="/mnt/raid_a/projects/show-a"),
    )
    built_manifests = {
        2: BuiltManifest(
            manifest_paths=("/mnt/raid_a/projects/show-b/a.txt",),
            decisions=(),
            json_payload={},
            summary_text="Manifest summary\nTotal decisions: 1",
        ),
        1: BuiltManifest(
            manifest_paths=("/mnt/raid_a/projects/show-a/a.txt",),
            decisions=(),
            json_payload={},
            summary_text="Manifest summary\nTotal decisions: 1",
        ),
    }
    manifest_results = {
        2: ManifestResult(
            manifest_paths=("/mnt/raid_a/projects/show-b/a.txt",),
            decisions=(),
            manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt",
            json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.json",
            summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.summary.txt",
        ),
        1: ManifestResult(
            manifest_paths=("/mnt/raid_a/projects/show-a/a.txt",),
            decisions=(),
            manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.manifest.txt",
            json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.manifest.json",
            summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-1.summary.txt",
        ),
    }
    backup_results = {
        2: BackupServiceResult(
            manifest_result=manifest_results[2],
            restic_result=ResticBackupResult(
                manifest_file_path=manifest_results[2].manifest_file_path,
                snapshot_id="snapshot-b",
                summary_payload={"message_type": "summary", "snapshot_id": "snapshot-b"},
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=1.0,
            ),
        ),
        1: BackupServiceResult(
            manifest_result=manifest_results[1],
            restic_result=ResticBackupResult(
                manifest_file_path=manifest_results[1].manifest_file_path,
                snapshot_id="snapshot-a",
                summary_payload={"message_type": "summary", "snapshot_id": "snapshot-a"},
                argv=("restic", "backup"),
                stdout="",
                stderr="",
                duration_seconds=1.0,
            ),
        ),
    }

    class FakeEngine:
        def dispose(self) -> None:
            calls.append(("dispose",))

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            calls.append(("roots-repo-init", session))

        def list_active(self):
            calls.append(("list_active",))
            return list(roots)

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        run_daily_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(run_daily_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        run_daily_module,
        "_current_run_timestamp",
        lambda: "20260317T081500Z",
    )
    monkeypatch.setattr(
        run_daily_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: calls.append(("build", session, root_id))
        or built_manifests[root_id],
    )

    def fake_write_manifest(*, built_manifest, output_dir, artifact_stem):
        calls.append(("write", built_manifest, output_dir, artifact_stem))
        root_id = 2 if artifact_stem.endswith("root-2") else 1
        return manifest_results[root_id]

    def fake_run_backup_from_manifest(request):
        calls.append(("backup", request))
        return backup_results[request.manifest_result.manifest_file_path.endswith("root-2.manifest.txt") and 2 or 1]

    monkeypatch.setattr(run_daily_module, "write_manifest", fake_write_manifest)
    monkeypatch.setattr(
        run_daily_module,
        "run_backup_from_manifest",
        fake_run_backup_from_manifest,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("list_active",) in calls
    assert calls.index(("build", "fake-session", 2)) < calls.index(
        ("build", "fake-session", 1)
    )
    assert ("build", "fake-session", 2) in calls
    assert ("build", "fake-session", 1) in calls
    assert (
        "write",
        built_manifests[2],
        run_daily_module.Path("/tmp/manifests"),
        "daily-20260317T081500Z-root-2",
    ) in calls
    assert (
        "write",
        built_manifests[1],
        run_daily_module.Path("/tmp/manifests"),
        "daily-20260317T081500Z-root-1",
    ) in calls
    backup_request_root_2 = run_daily_module.BackupServiceRequest(
        manifest_result=manifest_results[2],
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=7200,
    )
    backup_request_root_1 = run_daily_module.BackupServiceRequest(
        manifest_result=manifest_results[1],
        restic_binary="restic",
        restic_repository="/mnt/backup/repo",
        restic_password_env_var="RESTIC_PASSWORD",
        restic_timeout_seconds=7200,
    )
    assert ("backup", backup_request_root_2) in calls
    assert ("backup", backup_request_root_1) in calls
    assert calls.index(
        (
            "write",
            built_manifests[2],
            run_daily_module.Path("/tmp/manifests"),
            "daily-20260317T081500Z-root-2",
        )
    ) < calls.index(("backup", backup_request_root_2))
    assert calls.index(
        (
            "write",
            built_manifests[1],
            run_daily_module.Path("/tmp/manifests"),
            "daily-20260317T081500Z-root-1",
        )
    ) < calls.index(("backup", backup_request_root_1))
    assert calls.index(("backup", backup_request_root_2)) < calls.index(
        ("backup", backup_request_root_1)
    )
    assert captured.out.index("Daily backup root-id: 2") < captured.out.index(
        "Daily backup root-id: 1"
    )
    assert "Daily backup root-id: 2" in captured.out
    assert "root-path: /mnt/raid_a/projects/show-b" in captured.out
    assert "manifest-file: /tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt" in captured.out
    assert "snapshot-id: snapshot-b" in captured.out
    assert "Daily backup root-id: 1" in captured.out
    assert "root-path: /mnt/raid_a/projects/show-a" in captured.out
    assert "manifest-file: /tmp/manifests/daily-20260317T081500Z-root-1.manifest.txt" in captured.out
    assert "snapshot-id: snapshot-a" in captured.out
    assert "Daily run summary" in captured.out
    assert "roots-total: 2" in captured.out
    assert "roots-succeeded: 2" in captured.out
    assert "roots-failed: 0" in captured.out


def test_run_daily_returns_zero_when_no_active_roots(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace(
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(manifests_dir="/tmp/manifests"),
            restic=SimpleNamespace(
                binary="restic",
                repository="/mnt/backup/repo",
                password_env_var="RESTIC_PASSWORD",
                timeout_seconds=7200,
            ),
        )
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def list_active(self):
            return []

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        run_daily_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(run_daily_module, "RootsRepository", FakeRootsRepository)

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.strip() == "No active roots found."


def test_run_daily_partial_failure_continues_to_later_roots(monkeypatch, capsys) -> None:
    from backup_projects.cli import run_daily as run_daily_module

    fake_config = SimpleNamespace(
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(manifests_dir="/tmp/manifests"),
            restic=SimpleNamespace(
                binary="restic",
                repository="/mnt/backup/repo",
                password_env_var="RESTIC_PASSWORD",
                timeout_seconds=7200,
            ),
        )
    )
    calls: list[tuple] = []
    roots = (
        SimpleNamespace(id=1, path="/mnt/raid_a/projects/show-a"),
        SimpleNamespace(id=2, path="/mnt/raid_a/projects/show-b"),
    )
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt",
        json_manifest_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.manifest.json",
        summary_file_path="/tmp/manifests/daily-20260317T081500Z-root-2.summary.txt",
    )
    backup_result = BackupServiceResult(
        manifest_result=manifest_result,
        restic_result=ResticBackupResult(
            manifest_file_path=manifest_result.manifest_file_path,
            snapshot_id="snapshot-b",
            summary_payload={"message_type": "summary", "snapshot_id": "snapshot-b"},
            argv=("restic", "backup"),
            stdout="",
            stderr="",
            duration_seconds=1.0,
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def list_active(self):
            return list(roots)

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        run_daily_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        run_daily_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(run_daily_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(run_daily_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        run_daily_module,
        "_current_run_timestamp",
        lambda: "20260317T081500Z",
    )
    monkeypatch.setattr(
        run_daily_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: calls.append(("build", root_id)) or built_manifest,
    )

    def fake_write_manifest(*, built_manifest, output_dir, artifact_stem):
        calls.append(("write", artifact_stem))
        if artifact_stem.endswith("root-1"):
            raise ValueError("output_dir does not exist: /tmp/manifests")
        return manifest_result

    monkeypatch.setattr(run_daily_module, "write_manifest", fake_write_manifest)
    monkeypatch.setattr(
        run_daily_module,
        "run_backup_from_manifest",
        lambda request: calls.append(("backup", request.manifest_result.manifest_file_path))
        or backup_result,
    )

    exit_code = run_daily_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert ("build", 1) in calls
    assert ("build", 2) in calls
    assert ("write", "daily-20260317T081500Z-root-1") in calls
    assert ("write", "daily-20260317T081500Z-root-2") in calls
    assert ("backup", "/tmp/manifests/daily-20260317T081500Z-root-2.manifest.txt") in calls
    assert "output_dir does not exist: /tmp/manifests" in captured.err
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
