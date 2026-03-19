from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace


def test_runs_list_happy_path_respects_limit(monkeypatch, capsys) -> None:
    from backup_projects.cli import runs as runs_module
    from backup_projects.services.run_service import RunLifecycleRecord

    calls: list[tuple] = []
    fake_config = SimpleNamespace(
        app_path=runs_module.Path("/tmp/config/app.yaml"),
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(
                reports_dir="runtime/reports",
                logs_dir="runtime/logs",
            )
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            calls.append(("dispose",))

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(runs_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(runs_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(runs_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(runs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        runs_module,
        "list_runs",
        lambda *, session, limit: calls.append(("list_runs", session, limit))
        or (
            RunLifecycleRecord(
                id=7,
                run_type="daily",
                status="completed",
                started_at="2026-03-17T10:00:00+00:00",
                trigger_mode="cron",
                finished_at="2026-03-17T10:05:00+00:00",
            ),
        ),
    )

    exit_code = runs_module.main(["--config", "config/app.yaml", "list", "--limit", "5"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert ("list_runs", "fake-session", 5) in calls
    assert "Runs" in captured.out
    assert "id: 7" in captured.out
    assert "run-type: daily" in captured.out


def test_runs_show_happy_path(monkeypatch, capsys) -> None:
    from backup_projects.cli import runs as runs_module
    from backup_projects.services.run_service import RunLifecycleEvent, RunLifecycleRecord
    from backup_projects.services.run_visibility_service import ArtifactStatus, RunDetails

    fake_config = SimpleNamespace(
        app_path=runs_module.Path("/tmp/config/app.yaml"),
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(
                reports_dir="runtime/reports",
                logs_dir="runtime/logs",
            )
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(runs_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(runs_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(runs_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(runs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        runs_module,
        "get_run_details",
        lambda *, session, run_id, reports_dir, logs_dir: RunDetails(
            run=RunLifecycleRecord(
                id=3,
                run_type="backup",
                status="completed",
                started_at="2026-03-17T10:00:00+00:00",
                trigger_mode="manual",
                finished_at="2026-03-17T10:04:00+00:00",
            ),
            events=(
                RunLifecycleEvent(
                    id=1,
                    run_id=3,
                    event_time="2026-03-17T10:01:00+00:00",
                    level="INFO",
                    event_type="manifest_built",
                    message="Manifest built",
                    payload=None,
                ),
            ),
            report_json=ArtifactStatus(path="/tmp/runtime/reports/run-3/report.json", exists=True),
            report_text=ArtifactStatus(path="/tmp/runtime/reports/run-3/report.txt", exists=False),
            report_html=ArtifactStatus(path="/tmp/runtime/reports/run-3/report.html", exists=False),
            log_file=ArtifactStatus(path="/tmp/runtime/logs/run-3/run.log", exists=True),
        ),
    )

    exit_code = runs_module.main(["--config", "config/app.yaml", "show", "--run-id", "3"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Run" in captured.out
    assert "id: 3" in captured.out
    assert "manifest_built" in captured.out
    assert "report-json: /tmp/runtime/reports/run-3/report.json" in captured.out
    assert "report-json-exists: yes" in captured.out
    assert "log-file-exists: yes" in captured.out


def test_runs_show_missing_run_returns_predictable_failure(monkeypatch, capsys) -> None:
    from backup_projects.cli import runs as runs_module

    fake_config = SimpleNamespace(
        app_path=runs_module.Path("/tmp/config/app.yaml"),
        app_config=SimpleNamespace(
            runtime=SimpleNamespace(
                reports_dir="runtime/reports",
                logs_dir="runtime/logs",
            )
        ),
    )

    class FakeEngine:
        def dispose(self) -> None:
            return None

    @contextmanager
    def fake_session_scope(_session_factory):
        yield "fake-session"

    monkeypatch.setattr(runs_module, "load_config", lambda app_path, rules_path: fake_config)
    monkeypatch.setattr(runs_module, "create_engine_from_config", lambda config: FakeEngine())
    monkeypatch.setattr(runs_module, "create_session_factory", lambda engine: "fake-factory")
    monkeypatch.setattr(runs_module, "session_scope", fake_session_scope)
    monkeypatch.setattr(
        runs_module,
        "get_run_details",
        lambda **kwargs: (_ for _ in ()).throw(LookupError("Run not found for id: 404")),
    )

    exit_code = runs_module.main(["--config", "config/app.yaml", "show", "--run-id", "404"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Run not found for id: 404" in captured.err
