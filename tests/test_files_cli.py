from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace


def test_files_list_skipped_root_id_flow(monkeypatch, capsys) -> None:
    from backup_projects.cli import files as files_module
    from backup_projects.services.skipped_files_service import (
        SkippedFileEntry,
        SkippedFilesResult,
    )

    calls: list[tuple] = []

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(files_module, "load_config", lambda app_path, rules_path: SimpleNamespace())
    monkeypatch.setattr(files_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(files_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(files_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        files_module,
        "list_skipped_files",
        lambda *, session, root_id, root_path: calls.append((session, root_id, root_path))
        or SkippedFilesResult(
            root_id=7,
            root_path="/mnt/raid_a/show-a",
            skipped_files=(
                SkippedFileEntry(path="/mnt/raid_a/show-a/skipped.tmp", reason="excluded"),
            ),
        ),
    )

    exit_code = files_module.main(
        ["--config", "config/app.yaml", "list-skipped", "--root-id", "7"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert calls == [("fake-session", 7, None)]
    assert "Skipped files for root-id: 7" in captured.out
    assert "- /mnt/raid_a/show-a/skipped.tmp" in captured.out


def test_files_list_skipped_root_path_flow(monkeypatch, capsys) -> None:
    from backup_projects.cli import files as files_module
    from backup_projects.services.skipped_files_service import SkippedFilesResult

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(files_module, "load_config", lambda app_path, rules_path: SimpleNamespace())
    monkeypatch.setattr(files_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(files_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(files_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        files_module,
        "list_skipped_files",
        lambda *, session, root_id, root_path: SkippedFilesResult(
            root_id=8,
            root_path="/resolved/show-b",
            skipped_files=(),
        ),
    )

    exit_code = files_module.main(
        ["--config", "config/app.yaml", "list-skipped", "--root-path", "~/show-b"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "root-path: /resolved/show-b" in captured.out
    assert "- none" in captured.out


def test_files_list_skipped_invalid_selector_handling(capsys) -> None:
    from backup_projects.cli.files import main

    exit_code = main(["--config", "config/app.yaml", "list-skipped"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --root-id --root-path is required" in captured.err


def test_files_list_skipped_root_not_found_returns_predictable_failure(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import files as files_module

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(files_module, "load_config", lambda app_path, rules_path: SimpleNamespace())
    monkeypatch.setattr(files_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(files_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(files_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        files_module,
        "list_skipped_files",
        lambda **kwargs: (_ for _ in ()).throw(LookupError("Root not found for id: 404")),
    )

    exit_code = files_module.main(
        ["--config", "config/app.yaml", "list-skipped", "--root-id", "404"]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Root not found for id: 404" in captured.err
