from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain import ManifestResult
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.services.manifest_builder import BuiltManifest
from backup_projects.services.run_lock import AcquiredRunLock
from backup_projects.services.run_service import RunLifecycleRecord


def _make_backup_config() -> SimpleNamespace:
    return SimpleNamespace(
        app_path=Path("/tmp/config/app.yaml"),
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(locks_dir="runtime/locks"),
            restic=SimpleNamespace(
                binary="restic",
                repository="/mnt/backup/repo",
                password_env_var="RESTIC_PASSWORD",
                timeout_seconds=7200,
            ),
        ),
    )


def _patch_run_lifecycle_locking(monkeypatch, backup_module) -> None:
    monkeypatch.setattr(
        backup_module,
        "start_run",
        lambda **kwargs: RunLifecycleRecord(
            id=52,
            run_type="backup",
            status="running",
            started_at="2026-03-20T10:00:00+00:00",
            trigger_mode="manual",
            finished_at=None,
        ),
    )
    monkeypatch.setattr(
        backup_module,
        "try_acquire_run_lock",
        lambda **kwargs: AcquiredRunLock(
            run_id=52,
            lock_path="/tmp/runtime/locks/run.lock",
            _file_lock=None,
        ),
    )
    monkeypatch.setattr(
        backup_module,
        "finish_run",
        lambda *, session, run_id, status, now=None: RunLifecycleRecord(
            id=run_id,
            run_type="backup",
            status=status,
            started_at="2026-03-20T10:00:00+00:00",
            trigger_mode="manual",
            finished_at="2026-03-20T10:05:00+00:00",
        ),
    )


def test_backup_requires_root_selector(capsys) -> None:
    from backup_projects.cli.backup import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--output-dir",
            "runtime/manifests",
            "--artifact-stem",
            "daily-run",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --root-id --root-path is required" in captured.err


def test_backup_rejects_both_root_selectors(capsys) -> None:
    from backup_projects.cli.backup import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--root-path",
            "/mnt/raid_a/projects/show-a",
            "--output-dir",
            "runtime/manifests",
            "--artifact-stem",
            "daily-run",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "argument --root-path: not allowed with argument --root-id" in captured.err


def test_backup_requires_output_dir(capsys) -> None:
    from backup_projects.cli.backup import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--artifact-stem",
            "daily-run",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --output-dir" in captured.err


def test_backup_requires_artifact_stem(capsys) -> None:
    from backup_projects.cli.backup import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--output-dir",
            "runtime/manifests",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --artifact-stem" in captured.err


def test_backup_root_id_flow_writes_manifest_and_runs_backup(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    calls: list[tuple] = []
    root_record = SimpleNamespace(id=7, path="/mnt/raid_a/projects/show-a")
    built_manifest = BuiltManifest(
        manifest_paths=("/mnt/raid_a/projects/show-a/a.txt",),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 1",
    )
    manifest_result = ManifestResult(
        manifest_paths=("/mnt/raid_a/projects/show-a/a.txt",),
        decisions=(),
        manifest_file_path="/tmp/out/daily-run.manifest.txt",
        json_manifest_file_path="/tmp/out/daily-run.manifest.json",
        summary_file_path="/tmp/out/daily-run.summary.txt",
    )
    backup_result = BackupServiceResult(
        manifest_result=manifest_result,
        restic_result=ResticBackupResult(
            manifest_file_path=manifest_result.manifest_file_path,
            snapshot_id="snapshot-123",
            summary_payload={
                "message_type": "summary",
                "snapshot_id": "snapshot-123",
            },
            argv=("restic", "backup"),
            stdout="",
            stderr="",
            duration_seconds=1.5,
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            calls.append(("dispose",))

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            calls.append(("roots-repo-init", session))

        def get_by_id(self, root_id):
            calls.append(("get_by_id", root_id))
            return root_record

        def get_by_path(self, path):
            calls.append(("get_by_path", path))
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: calls.append(("build", session, root_id))
        or built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: calls.append(
            ("write", built_manifest, output_dir, artifact_stem)
        )
        or manifest_result,
    )
    monkeypatch.setattr(
        backup_module,
        "run_backup_from_manifest",
        lambda request: calls.append(("backup", request)) or backup_result,
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "daily-run",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_id", 7) in calls
    assert ("build", "fake-session", 7) in calls
    assert ("write", built_manifest, backup_module.Path("/tmp/out"), "daily-run") in calls
    assert (
        "backup",
        backup_module.BackupServiceRequest(
            manifest_result=manifest_result,
            restic_binary="restic",
            restic_repository="/mnt/backup/repo",
            restic_password_env_var="RESTIC_PASSWORD",
            restic_timeout_seconds=7200,
        ),
    ) in calls
    assert calls.index(
        ("write", built_manifest, backup_module.Path("/tmp/out"), "daily-run")
    ) < calls.index(
        (
            "backup",
            backup_module.BackupServiceRequest(
                manifest_result=manifest_result,
                restic_binary="restic",
                restic_repository="/mnt/backup/repo",
                restic_password_env_var="RESTIC_PASSWORD",
                restic_timeout_seconds=7200,
            ),
        )
    )
    assert "Backup for root-id: 7" in captured.out
    assert "root-path: /mnt/raid_a/projects/show-a" in captured.out
    assert "manifest-file: /tmp/out/daily-run.manifest.txt" in captured.out
    assert "json-manifest-file: /tmp/out/daily-run.manifest.json" in captured.out
    assert "summary-file: /tmp/out/daily-run.summary.txt" in captured.out
    assert "snapshot-id: snapshot-123" in captured.out


def test_backup_root_path_flow_resolves_path_before_lookup(monkeypatch, capsys) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    calls: list[tuple] = []
    root_record = SimpleNamespace(id=8, path="/mnt/raid_a/projects/show-b")
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path="/tmp/out/show-b.manifest.txt",
        json_manifest_file_path="/tmp/out/show-b.manifest.json",
        summary_file_path="/tmp/out/show-b.summary.txt",
    )
    backup_result = BackupServiceResult(
        manifest_result=manifest_result,
        restic_result=ResticBackupResult(
            manifest_file_path=manifest_result.manifest_file_path,
            snapshot_id="snapshot-456",
            summary_payload={
                "message_type": "summary",
                "snapshot_id": "snapshot-456",
            },
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

        def get_by_id(self, root_id):
            return None

        def get_by_path(self, path):
            calls.append(("get_by_path", path))
            return root_record

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "resolve_path",
        lambda path: SimpleNamespace(as_posix=lambda: "/resolved/show-b"),
    )
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: manifest_result,
    )
    monkeypatch.setattr(
        backup_module,
        "run_backup_from_manifest",
        lambda request: backup_result,
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-path",
            "~/show-b",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "show-b",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_path", "/resolved/show-b") in calls
    assert "snapshot-id: snapshot-456" in captured.out


def test_backup_empty_manifest_returns_successful_noop_and_prints_note(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    root_record = SimpleNamespace(id=12, path="/mnt/raid_a/projects/show-empty")
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path="/tmp/out/show-empty.manifest.txt",
        json_manifest_file_path="/tmp/out/show-empty.manifest.json",
        summary_file_path="/tmp/out/show-empty.summary.txt",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

        def get_by_path(self, path):
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: manifest_result,
    )
    monkeypatch.setattr(
        backup_module,
        "run_backup_from_manifest",
        lambda request: BackupServiceResult(
            manifest_result=manifest_result,
            restic_result=None,
            message="Backup skipped: manifest include set is empty",
        ),
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "12",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "show-empty",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "backup-note: Backup skipped: manifest include set is empty" in captured.out
    assert "snapshot-id:" not in captured.out


def test_backup_write_manifest_failure_is_operational_failure(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    root_record = SimpleNamespace(id=9, path="/mnt/raid_a/projects/show-c")
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

        def get_by_path(self, path):
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda **kwargs: (_ for _ in ()).throw(
            ValueError("output_dir does not exist: /tmp/out")
        ),
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "9",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "show-c",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "output_dir does not exist: /tmp/out"


def test_backup_runtime_failure_returns_exit_code_1(monkeypatch, capsys) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    root_record = SimpleNamespace(id=10, path="/mnt/raid_a/projects/show-d")
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path="/tmp/out/show-d.manifest.txt",
        json_manifest_file_path="/tmp/out/show-d.manifest.json",
        summary_file_path="/tmp/out/show-d.summary.txt",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

        def get_by_path(self, path):
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: manifest_result,
    )
    monkeypatch.setattr(
        backup_module,
        "run_backup_from_manifest",
        lambda request: (_ for _ in ()).throw(RuntimeError("restic failed")),
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "10",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "show-d",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "restic failed"


def test_backup_service_value_error_is_operational_failure(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()
    root_record = SimpleNamespace(id=11, path="/mnt/raid_a/projects/show-e")
    built_manifest = BuiltManifest(
        manifest_paths=(),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 0",
    )
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path="/tmp/out/show-e.manifest.txt",
        json_manifest_file_path="/tmp/out/show-e.manifest.json",
        summary_file_path="/tmp/out/show-e.summary.txt",
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

        def get_by_path(self, path):
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)
    _patch_run_lifecycle_locking(monkeypatch, backup_module)
    monkeypatch.setattr(
        backup_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        backup_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: manifest_result,
    )
    monkeypatch.setattr(
        backup_module,
        "run_backup_from_manifest",
        lambda request: (_ for _ in ()).throw(
            ValueError("manifest_result.manifest_file_path must not be empty")
        ),
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "11",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "show-e",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "manifest_result.manifest_file_path must not be empty"


def test_backup_lookup_failure_returns_exit_code_2(monkeypatch, capsys) -> None:
    from backup_projects.cli import backup as backup_module

    fake_config = _make_backup_config()

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return None

        def get_by_path(self, path):
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        backup_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        backup_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(backup_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(backup_module, "RootsRepository", FakeRootsRepository)

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "999",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "missing",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.strip() == "Root not found for id: 999"


def test_backup_config_error_returns_exit_code_2(monkeypatch, capsys) -> None:
    from backup_projects.cli import backup as backup_module
    from backup_projects.config import ConfigValidationError

    monkeypatch.setattr(
        backup_module,
        "load_config",
        lambda app_path, rules_path: (_ for _ in ()).throw(
            ConfigValidationError("bad config")
        ),
    )

    exit_code = backup_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "daily-run",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.strip() == "bad config"
