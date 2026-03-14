from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from backup_projects.services.structural_scan_sync_service import StructuralScanSyncResult


def test_scan_structure_requires_target_selector(capsys) -> None:
    from backup_projects.cli.scan_structure import main

    exit_code = main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --root-id --path is required" in captured.err


def test_scan_structure_rejects_both_root_id_and_path(capsys) -> None:
    from backup_projects.cli.scan_structure import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "1",
            "--path",
            "/mnt/raid_a/show-root",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "not allowed with argument" in captured.err


def test_scan_structure_root_id_flow_calls_services_and_prints_summary(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    calls: list[tuple[str, object]] = []
    dispose_calls: list[str] = []
    root_record = SimpleNamespace(id=7, name="show-root", path="/mnt/raid_a/show-root")
    fake_config = SimpleNamespace(
        rules_config=SimpleNamespace(allowed_extensions=["prproj", "aep"])
    )

    class FakeEngine:
        def dispose(self) -> None:
            dispose_calls.append("disposed")

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            calls.append(("repo-init", session))

        def get_by_id(self, root_id):
            calls.append(("get_by_id", root_id))
            return root_record

        def get_by_path(self, path):
            calls.append(("get_by_path", path))
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_scan_root_structure(*, root_path, allowed_extensions):
        calls.append(("scan", root_path, tuple(allowed_extensions)))
        return "scan-result"

    def fake_sync_structural_scan_result(*, session, root_id, scan_result, synced_at):
        calls.append(("sync", session, root_id, scan_result, synced_at))
        return _make_sync_result(root_id=root_id, root_path=root_record.path)

    monkeypatch.setattr(
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(scan_structure_module, "scan_root_structure", fake_scan_root_structure)
    monkeypatch.setattr(
        scan_structure_module,
        "sync_structural_scan_result",
        fake_sync_structural_scan_result,
    )

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_id", 7) in calls
    assert ("scan", "/mnt/raid_a/show-root", ("prproj", "aep")) in calls
    sync_call = next(call for call in calls if call[0] == "sync")
    assert sync_call[1] == "fake-session"
    assert sync_call[2] == 7
    assert sync_call[3] == "scan-result"
    assert isinstance(sync_call[4], str)
    assert "Scanned target: show-root (/mnt/raid_a/show-root)" in captured.out
    assert "root-id: 7" in captured.out
    assert "project-dirs-created: 2" in captured.out
    assert "project-files-marked-missing: 4" in captured.out
    assert dispose_calls == ["disposed"]


def test_scan_structure_path_flow_resolves_path_and_prints_summary(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    calls: list[tuple[str, object]] = []
    root_record = SimpleNamespace(id=8, name="show-root", path="/mnt/raid_a/show-root")
    fake_config = SimpleNamespace(rules_config=SimpleNamespace(allowed_extensions=["prproj"]))

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            calls.append(("repo-init", session))

        def get_by_id(self, root_id):
            calls.append(("get_by_id", root_id))
            return None

        def get_by_path(self, path):
            calls.append(("get_by_path", path))
            return root_record

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_scan_root_structure(*, root_path, allowed_extensions):
        calls.append(("scan", root_path, tuple(allowed_extensions)))
        return "scan-result"

    def fake_sync_structural_scan_result(*, session, root_id, scan_result, synced_at):
        calls.append(("sync", session, root_id, scan_result, synced_at))
        return _make_sync_result(root_id=root_id, root_path=root_record.path)

    monkeypatch.setattr(
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        scan_structure_module,
        "resolve_path",
        lambda path: Path("/resolved/show-root"),
    )
    monkeypatch.setattr(scan_structure_module, "scan_root_structure", fake_scan_root_structure)
    monkeypatch.setattr(
        scan_structure_module,
        "sync_structural_scan_result",
        fake_sync_structural_scan_result,
    )

    exit_code = scan_structure_module.main(
        ["--config", "config/app.yaml", "--path", "~/raid_a/show-root"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_path", "/resolved/show-root") in calls
    assert ("scan", "/mnt/raid_a/show-root", ("prproj",)) in calls
    assert "Scanned target: show-root (/mnt/raid_a/show-root)" in captured.out


def test_scan_structure_returns_one_for_unknown_root_id(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    fake_config = SimpleNamespace(rules_config=SimpleNamespace(allowed_extensions=["prproj"]))

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
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "Root not found for id: 7"


def test_scan_structure_returns_one_for_unknown_path(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    fake_config = SimpleNamespace(rules_config=SimpleNamespace(allowed_extensions=["prproj"]))

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
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        scan_structure_module,
        "resolve_path",
        lambda path: Path("/resolved/show-root"),
    )

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--path", "show-root"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "Root not found for path: /resolved/show-root"


def test_scan_structure_returns_two_for_config_error(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module
    from backup_projects.config import ConfigError

    monkeypatch.setattr(
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: (_ for _ in ()).throw(ConfigError("bad config")),
    )

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.strip() == "bad config"


def test_scan_structure_returns_one_for_runtime_error(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    fake_config = SimpleNamespace(rules_config=SimpleNamespace(allowed_extensions=["prproj"]))
    root_record = SimpleNamespace(id=7, name="show-root", path="/mnt/raid_a/show-root")

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

    def fake_scan_root_structure(*, root_path, allowed_extensions):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(scan_structure_module, "scan_root_structure", fake_scan_root_structure)

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "boom"


def test_scan_structure_returns_one_for_sync_error(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_structure as scan_structure_module

    fake_config = SimpleNamespace(rules_config=SimpleNamespace(allowed_extensions=["prproj"]))
    root_record = SimpleNamespace(id=7, name="show-root", path="/mnt/raid_a/show-root")

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

    def fake_scan_root_structure(*, root_path, allowed_extensions):
        return "scan-result"

    def fake_sync_structural_scan_result(*, session, root_id, scan_result, synced_at):
        raise RuntimeError("sync boom")

    monkeypatch.setattr(
        scan_structure_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_structure_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_structure_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_structure_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(scan_structure_module, "scan_root_structure", fake_scan_root_structure)
    monkeypatch.setattr(
        scan_structure_module,
        "sync_structural_scan_result",
        fake_sync_structural_scan_result,
    )

    exit_code = scan_structure_module.main(["--config", "config/app.yaml", "--root-id", "7"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "sync boom"


def _make_sync_result(*, root_id: int, root_path: str) -> StructuralScanSyncResult:
    return StructuralScanSyncResult(
        root_id=root_id,
        root_path=root_path,
        synced_at="2026-03-14T10:00:00+00:00",
        scanned_project_dir_count=5,
        created_project_dir_count=2,
        updated_project_dir_count=1,
        reactivated_project_dir_count=1,
        marked_missing_project_dir_count=1,
        scanned_project_file_count=11,
        created_project_file_count=3,
        updated_project_file_count=2,
        reactivated_project_file_count=2,
        marked_missing_project_file_count=4,
    )
