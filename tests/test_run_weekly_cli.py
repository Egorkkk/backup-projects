def test_run_weekly_requires_config(capsys) -> None:
    from backup_projects.cli.run_weekly import main

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "the following arguments are required: --config" in captured.err


def test_run_weekly_returns_placeholder_exit_code_and_message(capsys) -> None:
    from backup_projects.cli.run_weekly import main

    exit_code = main(["--config", "config/app.yaml"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.strip() == "run-weekly is not implemented in v1 baseline"
