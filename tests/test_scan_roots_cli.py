from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from backup_projects.services.root_discovery_service import RootDiscoveryResult


def test_scan_roots_requires_config(capsys) -> None:
    from backup_projects.cli.scan_roots import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_scan_roots_loads_config_invokes_service_and_prints_summary(
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import scan_roots as scan_roots_module

    calls: list[tuple[str, str, str, object]] = []
    dispose_calls: list[str] = []

    fake_config = SimpleNamespace(
        app_config=SimpleNamespace(
            raid_roots=[
                SimpleNamespace(name="raid_a", path="/mnt/raid_a/projects", enabled=True),
                SimpleNamespace(name="raid_b", path="/mnt/raid_b/projects", enabled=False),
            ]
        )
    )

    class FakeEngine:
        def dispose(self) -> None:
            dispose_calls.append("disposed")

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_discover_and_sync_roots(*, session, raid_name, raid_path, discovered_at):
        calls.append((raid_name, raid_path, discovered_at, session))
        return RootDiscoveryResult(
            discovered=(),
            created=(),
            marked_missing=(),
            reactivated=(),
            unchanged_present=(),
        )

    monkeypatch.setattr(scan_roots_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(scan_roots_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(scan_roots_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(scan_roots_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_roots_module, "discover_and_sync_roots", fake_discover_and_sync_roots)

    exit_code = scan_roots_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0][0] == "raid_a"
    assert calls[0][1] == "/mnt/raid_a/projects"
    assert calls[0][3] == "fake-session"
    assert "Scanned target: raid_a (/mnt/raid_a/projects)" in captured.out
    assert "discovered: 0" in captured.out
    assert "created: 0" in captured.out
    assert "reactivated: 0" in captured.out
    assert "marked-missing: 0" in captured.out
    assert "unchanged-present: 0" in captured.out
    assert dispose_calls == ["disposed"]


def test_scan_roots_returns_nonzero_and_prints_concise_error(monkeypatch, capsys) -> None:
    from backup_projects.cli import scan_roots as scan_roots_module

    fake_config = SimpleNamespace(
        app_config=SimpleNamespace(
            raid_roots=[SimpleNamespace(name="raid_a", path="/mnt/raid_a/projects", enabled=True)]
        )
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    def fake_discover_and_sync_roots(*, session, raid_name, raid_path, discovered_at):
        raise RuntimeError("boom")

    monkeypatch.setattr(scan_roots_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(scan_roots_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(scan_roots_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(scan_roots_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(scan_roots_module, "discover_and_sync_roots", fake_discover_and_sync_roots)

    exit_code = scan_roots_module.main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err.strip() == "boom"
