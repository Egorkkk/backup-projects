from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.config import ConfigError
from backup_projects.services.project_dir_scan_service import ProjectDirIncrementalScanResult


def test_scan_project_dirs_requires_selector(capsys) -> None:
    from backup_projects.cli.scan_project_dirs import main

    exit_code = main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --project-dir-id --root-id is required" in captured.err


def test_scan_project_dirs_rejects_both_selectors(capsys) -> None:
    from backup_projects.cli.scan_project_dirs import main

    exit_code = main(
        [
            "--config",
            "config/app.yaml",
            "--project-dir-id",
            "7",
            "--root-id",
            "3",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "not allowed with argument" in captured.err


def test_scan_project_dirs_project_dir_id_flow_calls_service_and_prints_summary(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    calls: list[tuple] = []
    dispose_calls: list[str] = []
    fake_config = SimpleNamespace()
    project_dir_record = SimpleNamespace(id=7)

    class FakeEngine:
        def dispose(self) -> None:
            dispose_calls.append("disposed")

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            calls.append(("project-dirs-repo-init", session))

        def get_by_id(self, project_dir_id):
            calls.append(("get_by_id", project_dir_id))
            return project_dir_record

        def list_active_by_root(self, root_id):
            calls.append(("list_active_by_root", root_id))
            return ()

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_scan_and_sync_project_dir(*, session, project_dir_id, scanned_at):
        calls.append(("scan_and_sync", session, project_dir_id, scanned_at))
        return _make_result(
            project_dir_id=project_dir_id,
            root_id=3,
            project_dir_relative_path="shows/edit",
            project_dir_path="/mnt/raid_a/shows/edit",
            project_dir_present=True,
            scanned_file_count=5,
            new_file_count=1,
            changed_file_count=2,
            reactivated_file_count=0,
            missing_file_count=1,
            unchanged_file_count=1,
        )

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "scan_and_sync_project_dir",
        fake_scan_and_sync_project_dir,
    )

    exit_code = scan_project_dirs_module.main(
        ["--config", "config/app.yaml", "--project-dir-id", "7"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("get_by_id", 7) in calls
    scan_call = next(call for call in calls if call[0] == "scan_and_sync")
    assert scan_call[1] == "fake-session"
    assert scan_call[2] == 7
    assert isinstance(scan_call[3], str)
    assert "Scanned target: shows/edit" in captured.out
    assert "project-dir-id: 7" in captured.out
    assert "root-id: 3" in captured.out
    assert "project-dir-path: /mnt/raid_a/shows/edit" in captured.out
    assert "project-dir-present: true" in captured.out
    assert "files-scanned: 5" in captured.out
    assert "files-new: 1" in captured.out
    assert "files-changed: 2" in captured.out
    assert "files-missing: 1" in captured.out
    assert "files-unchanged: 1" in captured.out
    assert dispose_calls == ["disposed"]


def test_scan_project_dirs_root_id_flow_calls_service_for_each_project_dir(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    calls: list[tuple] = []
    fake_config = SimpleNamespace()
    root_record = SimpleNamespace(id=5)
    project_dir_records = (
        SimpleNamespace(id=11),
        SimpleNamespace(id=12),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            calls.append(("project-dirs-repo-init", session))

        def get_by_id(self, project_dir_id):
            calls.append(("get_by_id", project_dir_id))
            return None

        def list_active_by_root(self, root_id):
            calls.append(("list_active_by_root", root_id))
            return project_dir_records

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            calls.append(("roots-repo-init", session))

        def get_by_id(self, root_id):
            calls.append(("root_get_by_id", root_id))
            return root_record

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_scan_and_sync_project_dir(*, session, project_dir_id, scanned_at):
        calls.append(("scan_and_sync", session, project_dir_id, scanned_at))
        return _make_result(
            project_dir_id=project_dir_id,
            root_id=5,
            project_dir_relative_path="" if project_dir_id == 11 else "shows/shot_01",
            project_dir_path=(
                "/mnt/raid_a/root"
                if project_dir_id == 11
                else "/mnt/raid_a/root/shows/shot_01"
            ),
            project_dir_present=project_dir_id == 11,
            scanned_file_count=project_dir_id,
            new_file_count=0,
            changed_file_count=1,
            reactivated_file_count=0,
            missing_file_count=0,
            unchanged_file_count=2,
        )

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )
    monkeypatch.setattr(scan_project_dirs_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "scan_and_sync_project_dir",
        fake_scan_and_sync_project_dir,
    )

    exit_code = scan_project_dirs_module.main(
        ["--config", "config/app.yaml", "--root-id", "5"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("root_get_by_id", 5) in calls
    assert ("list_active_by_root", 5) in calls
    scan_calls = [call for call in calls if call[0] == "scan_and_sync"]
    assert [call[2] for call in scan_calls] == [11, 12]
    assert scan_calls[0][3] == scan_calls[1][3]
    assert "Scanned target: root" in captured.out
    assert "Scanned target: shows/shot_01" in captured.out
    assert "\n\nScanned target: shows/shot_01" in captured.out


def test_scan_project_dirs_returns_one_for_unknown_project_dir_id(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    fake_config = SimpleNamespace()

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, project_dir_id):
            return None

        def list_active_by_root(self, root_id):
            return ()

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )

    exit_code = scan_project_dirs_module.main(
        ["--config", "config/app.yaml", "--project-dir-id", "7"]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "Project dir not found for id: 7"


def test_scan_project_dirs_returns_one_for_unknown_root_id(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    fake_config = SimpleNamespace()

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return None

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, project_dir_id):
            return None

        def list_active_by_root(self, root_id):
            return ()

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_project_dirs_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )

    exit_code = scan_project_dirs_module.main(["--config", "config/app.yaml", "--root-id", "9"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "Root not found for id: 9"


def test_scan_project_dirs_returns_zero_when_root_has_no_active_project_dirs(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    fake_config = SimpleNamespace()
    root_record = SimpleNamespace(id=5)

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeRootsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, root_id):
            return root_record

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, project_dir_id):
            return None

        def list_active_by_root(self, root_id):
            return ()

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_project_dirs_module, "RootsRepository", FakeRootsRepository)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )

    exit_code = scan_project_dirs_module.main(["--config", "config/app.yaml", "--root-id", "5"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.strip() == "No active project dirs found for root-id: 5"


def test_scan_project_dirs_returns_two_for_config_error(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: (_ for _ in ()).throw(ConfigError("bad config")),
    )

    exit_code = scan_project_dirs_module.main(
        ["--config", "config/app.yaml", "--project-dir-id", "7"]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.err.strip() == "bad config"


def test_scan_project_dirs_returns_one_for_service_exception(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_project_dirs as scan_project_dirs_module

    fake_config = SimpleNamespace()
    project_dir_record = SimpleNamespace(id=7)

    class FakeEngine:
        def dispose(self) -> None:
            return None

    class FakeProjectDirsRepository:
        def __init__(self, session) -> None:
            return None

        def get_by_id(self, project_dir_id):
            return project_dir_record

        def list_active_by_root(self, root_id):
            return ()

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_scan_and_sync_project_dir(*, session, project_dir_id, scanned_at):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        scan_project_dirs_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_engine_from_config",
        lambda config: FakeEngine(),
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "create_session_factory",
        lambda engine: "fake-factory",
    )
    monkeypatch.setattr(scan_project_dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        scan_project_dirs_module,
        "ProjectDirsRepository",
        FakeProjectDirsRepository,
    )
    monkeypatch.setattr(
        scan_project_dirs_module,
        "scan_and_sync_project_dir",
        fake_scan_and_sync_project_dir,
    )

    exit_code = scan_project_dirs_module.main(
        ["--config", "config/app.yaml", "--project-dir-id", "7"]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "boom"


def _make_result(
    *,
    project_dir_id: int,
    root_id: int,
    project_dir_relative_path: str,
    project_dir_path: str,
    project_dir_present: bool,
    scanned_file_count: int,
    new_file_count: int,
    changed_file_count: int,
    reactivated_file_count: int,
    missing_file_count: int,
    unchanged_file_count: int,
) -> ProjectDirIncrementalScanResult:
    return ProjectDirIncrementalScanResult(
        project_dir_id=project_dir_id,
        root_id=root_id,
        project_dir_relative_path=project_dir_relative_path,
        project_dir_path=project_dir_path,
        scanned_at="2026-03-14T12:00:00+00:00",
        project_dir_present=project_dir_present,
        scanned_file_count=scanned_file_count,
        new_file_count=new_file_count,
        changed_file_count=changed_file_count,
        reactivated_file_count=reactivated_file_count,
        missing_file_count=missing_file_count,
        unchanged_file_count=unchanged_file_count,
    )
