from __future__ import annotations

from types import SimpleNamespace


def test_cli_app_without_command_prints_help(capsys) -> None:
    from backup_projects.cli.app import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "backup-projects command-line interface." in captured.out


def test_cli_app_dispatches_representative_commands(monkeypatch) -> None:
    from backup_projects.cli import app as app_module

    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        app_module.init_db,
        "handle",
        lambda args: calls.append(("init-db", args)) or 11,
    )
    monkeypatch.setattr(
        app_module.scan_roots,
        "handle",
        lambda args: calls.append(("scan-roots", args)) or 12,
    )
    monkeypatch.setattr(
        app_module.run_daily,
        "handle",
        lambda args: calls.append(("run-daily", args)) or 13,
    )
    monkeypatch.setattr(
        app_module.rules,
        "handle",
        lambda args: calls.append(("rules", args)) or 14,
    )
    monkeypatch.setattr(
        app_module.include,
        "handle",
        lambda args: calls.append(("include", args)) or 15,
    )
    monkeypatch.setattr(
        app_module.runs,
        "handle",
        lambda args: calls.append(("runs", args)) or 16,
    )
    monkeypatch.setattr(
        app_module.files,
        "handle",
        lambda args: calls.append(("files", args)) or 17,
    )
    monkeypatch.setattr(
        app_module.roots,
        "handle",
        lambda args: calls.append(("roots", args)) or 18,
    )
    monkeypatch.setattr(
        app_module.dirs,
        "handle",
        lambda args: calls.append(("dirs", args)) or 19,
    )

    assert app_module.main(["init-db"]) == 11
    assert app_module.main(["scan-roots", "--config", "config/app.yaml"]) == 12
    assert app_module.main(["run-daily", "--config", "config/app.yaml"]) == 13
    assert app_module.main(["rules", "--config", "config/app.yaml", "list"]) == 14
    assert (
        app_module.main(
            ["include", "--config", "config/app.yaml", "list", "--root-id", "7"]
        )
        == 15
    )
    assert app_module.main(["runs", "--config", "config/app.yaml", "list"]) == 16
    assert (
        app_module.main(
            ["files", "--config", "config/app.yaml", "list-skipped", "--root-id", "7"]
        )
        == 17
    )
    assert app_module.main(["roots", "--config", "config/app.yaml", "list"]) == 18
    assert app_module.main(["dirs", "--config", "config/app.yaml", "list"]) == 19

    assert [name for name, _args in calls] == [
        "init-db",
        "scan-roots",
        "run-daily",
        "rules",
        "include",
        "runs",
        "files",
        "roots",
        "dirs",
    ]


def test_cli_app_dispatches_run_weekly_placeholder(capsys) -> None:
    from backup_projects.cli.app import main

    exit_code = main(["run-weekly", "--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.strip() == "run-weekly is not implemented in v1 baseline"


def test_cli_app_dispatches_doctor_placeholder(capsys) -> None:
    from backup_projects.cli.app import main

    exit_code = main(["doctor", "--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.strip() == "doctor is not implemented in v1 baseline"
