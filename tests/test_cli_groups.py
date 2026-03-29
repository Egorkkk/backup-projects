from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace


def test_include_group_delegates_add_file(monkeypatch) -> None:
    from backup_projects.cli import include as include_module

    calls: list[list[str]] = []

    monkeypatch.setattr(
        include_module.include_add_file,
        "main",
        lambda argv=None: calls.append(list(argv or [])) or 21,
    )

    exit_code = include_module.main(
        [
            "--config",
            "config/app.yaml",
            "add-file",
            "--root-id",
            "7",
            "--force-include",
            "--disabled",
            "incoming/file.prproj",
        ]
    )

    assert exit_code == 21
    assert calls == [
        [
            "--config",
            "config/app.yaml",
            "--root-id",
            "7",
            "--force-include",
            "--disabled",
            "incoming/file.prproj",
        ]
    ]


def test_include_group_delegates_enable(monkeypatch) -> None:
    from backup_projects.cli import include as include_module

    calls: list[list[str]] = []

    monkeypatch.setattr(
        include_module.include_enable,
        "main",
        lambda argv=None: calls.append(list(argv or [])) or 22,
    )

    exit_code = include_module.main(
        [
            "--config",
            "config/app.yaml",
            "enable",
            "--manual-include-id",
            "42",
        ]
    )

    assert exit_code == 22
    assert calls == [["--config", "config/app.yaml", "--manual-include-id", "42"]]


def test_roots_list_uses_roots_service_read_seam(monkeypatch, capsys) -> None:
    from backup_projects.cli import roots as roots_module

    fake_config = SimpleNamespace()
    calls: list[tuple] = []

    class FakeEngine:
        def dispose(self) -> None:
            calls.append(("dispose",))

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(roots_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(roots_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(roots_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(roots_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        roots_module,
        "build_roots_page_view",
        lambda *, session, status, rescan: calls.append((session, status, rescan))
        or SimpleNamespace(
            rows=(
                SimpleNamespace(
                    id=5,
                    raid_name="raid_a",
                    name="show-a",
                    path="/mnt/raid_a/show-a",
                    status="active",
                    needs_structural_rescan=True,
                    last_seen_at="2026-03-20T10:00:00+00:00",
                ),
            )
        ),
    )

    exit_code = roots_module.main(["--config", "config/app.yaml", "list"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("fake-session", None, None) in calls
    assert "Roots" in captured.out
    assert "id: 5" in captured.out
    assert "raid-name: raid_a" in captured.out
    assert "needs-structural-rescan: yes" in captured.out


def test_dirs_list_uses_project_dirs_service_read_seam(monkeypatch, capsys) -> None:
    from backup_projects.cli import dirs as dirs_module

    fake_config = SimpleNamespace()
    calls: list[tuple] = []

    class FakeEngine:
        def dispose(self) -> None:
            calls.append(("dispose",))

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(dirs_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(dirs_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(dirs_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(dirs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        dirs_module,
        "build_project_dirs_page_view",
        lambda *, session: calls.append((session,))
        or SimpleNamespace(
            rows=(
                SimpleNamespace(
                    id=9,
                    root_id=5,
                    root_name="show-a",
                    root_path="/mnt/raid_a/show-a",
                    relative_path="edit/ProjectAlpha",
                    name="ProjectAlpha",
                    dir_type="premiere",
                    status="active",
                    last_seen_at="2026-03-20T10:05:00+00:00",
                ),
            )
        ),
    )

    exit_code = dirs_module.main(["--config", "config/app.yaml", "list"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("fake-session",) in calls
    assert "Project directories" in captured.out
    assert "id: 9" in captured.out
    assert "root-id: 5" in captured.out
    assert "dir-type: premiere" in captured.out


def test_doctor_placeholder_behavior(capsys) -> None:
    from backup_projects.cli.doctor import main

    exit_code = main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.strip() == "doctor is not implemented in v1 baseline"
