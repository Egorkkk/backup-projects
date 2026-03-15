from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.domain import ManifestResult
from backup_projects.services.manifest_builder import BuiltManifest


def test_dry_run_requires_root_selector(capsys) -> None:
    from backup_projects.cli.dry_run import main

    exit_code = main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --root-id --root-path is required" in captured.err


def test_dry_run_requires_output_flags_together(capsys) -> None:
    from backup_projects.cli.dry_run import main

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
    assert "--output-dir and --artifact-stem must be provided together" in captured.err


def test_dry_run_root_id_flow_prints_summary(monkeypatch, capsys) -> None:
    from backup_projects.cli import dry_run as dry_run_module

    fake_config = SimpleNamespace()
    calls: list[tuple] = []
    root_record = SimpleNamespace(id=7, path="/mnt/raid_a/projects/show-a")
    built_manifest = BuiltManifest(
        manifest_paths=("/mnt/raid_a/projects/show-a/a.txt",),
        decisions=(),
        json_payload={},
        summary_text="Manifest summary\nTotal decisions: 1",
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
        dry_run_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(dry_run_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(dry_run_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        dry_run_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: calls.append(("build", session, root_id))
        or built_manifest,
    )

    exit_code = dry_run_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_id", 7) in calls
    assert ("build", "fake-session", 7) in calls
    assert "Dry run for root-id: 7" in captured.out
    assert "root-path: /mnt/raid_a/projects/show-a" in captured.out
    assert "Manifest summary\nTotal decisions: 1" in captured.out
    assert "manifest-file:" not in captured.out


def test_dry_run_root_path_flow_can_write_artifacts(monkeypatch, capsys) -> None:
    from backup_projects.cli import dry_run as dry_run_module

    fake_config = SimpleNamespace()
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
        manifest_file_path="/tmp/out/test.manifest.txt",
        json_manifest_file_path="/tmp/out/test.manifest.json",
        summary_file_path="/tmp/out/test.summary.txt",
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
        dry_run_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(dry_run_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(dry_run_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        dry_run_module,
        "resolve_path",
        lambda path: SimpleNamespace(as_posix=lambda: "/resolved/show-b"),
    )
    monkeypatch.setattr(
        dry_run_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        dry_run_module,
        "write_manifest",
        lambda *, built_manifest, output_dir, artifact_stem: calls.append(
            ("write", built_manifest, output_dir, artifact_stem)
        )
        or manifest_result,
    )

    exit_code = dry_run_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-path",
            "~/show-b",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "test",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_path", "/resolved/show-b") in calls
    assert ("write", built_manifest, dry_run_module.Path("/tmp/out"), "test") in calls
    assert "manifest-file: /tmp/out/test.manifest.txt" in captured.out
    assert "json-manifest-file: /tmp/out/test.manifest.json" in captured.out
    assert "summary-file: /tmp/out/test.summary.txt" in captured.out


def test_dry_run_write_manifest_value_error_is_operational_failure(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import dry_run as dry_run_module

    fake_config = SimpleNamespace()
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
        dry_run_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        dry_run_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(dry_run_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(dry_run_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        dry_run_module,
        "build_root_dry_run_manifest",
        lambda *, session, root_id: built_manifest,
    )
    monkeypatch.setattr(
        dry_run_module,
        "write_manifest",
        lambda **kwargs: (_ for _ in ()).throw(
            ValueError("output_dir does not exist: /tmp/out")
        ),
    )

    exit_code = dry_run_module.main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "9",
            "--output-dir",
            "/tmp/out",
            "--artifact-stem",
            "test",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "output_dir does not exist: /tmp/out"
