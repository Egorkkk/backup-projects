from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
)
from backup_projects.repositories.rules_repo import RulesRepository


@pytest.fixture
def cli_db(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "rules-cli.sqlite3")
    create_schema(engine)
    yield engine
    engine.dispose()


def test_rules_list_prints_sections_for_rules_and_patterns(
    cli_db,
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    session_factory = create_session_factory(cli_db)
    with rules_module.session_scope(session_factory) as session:
        repo = RulesRepository(session)
        repo.create_extension_rule(
            extension="aaf",
            enabled=True,
            max_size_bytes=100,
            oversize_action="skip",
            created_at="2026-03-15T10:00:00+00:00",
            updated_at="2026-03-15T10:00:00+00:00",
        )
        repo.create_excluded_pattern(
            pattern_type="glob",
            pattern_value="**/Cache/**",
            enabled=False,
            created_at="2026-03-15T10:00:00+00:00",
            updated_at="2026-03-15T10:00:00+00:00",
        )

    exit_code = rules_module.main(["--config", "config/app.yaml", "list"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Extension rules" in captured.out
    assert "extension: aaf" in captured.out
    assert "enabled: true" in captured.out
    assert "oversize-action: skip" in captured.out
    assert "Excluded patterns" in captured.out
    assert "pattern-type: glob" in captured.out
    assert "pattern-value: **/Cache/**" in captured.out
    assert "enabled: false" in captured.out


def test_rules_list_prints_empty_section_messages(cli_db, monkeypatch, capsys) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    exit_code = rules_module.main(["--config", "config/app.yaml", "list"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Extension rules" in captured.out
    assert "No extension rules found." in captured.out
    assert "Excluded patterns" in captured.out
    assert "No excluded patterns found." in captured.out


def test_rules_add_and_update_extension_commands_mutate_rule_rows(
    cli_db,
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "add-extension",
            ".AAF",
            "--oversize-action",
            "warn",
            "--max-size-bytes",
            "512",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Created extension rule:" in captured.out
    assert "extension: aaf" in captured.out
    assert "oversize-action: warn" in captured.out

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "update-extension",
            "aaf",
            "--oversize-action",
            "include",
            "--clear-max-size",
            "--disabled",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Updated extension rule: aaf" in captured.out
    assert "enabled: false" in captured.out
    assert "max-size-bytes: None" in captured.out
    assert "oversize-action: include" in captured.out

    session_factory = create_session_factory(cli_db)
    with rules_module.session_scope(session_factory) as session:
        refreshed_rule = RulesRepository(session).get_extension_rule("aaf")

    assert refreshed_rule is not None
    assert refreshed_rule.enabled is False
    assert refreshed_rule.max_size_bytes is None
    assert refreshed_rule.oversize_action == "include"


def test_rules_add_and_disable_exclude_commands_mutate_pattern_rows(
    cli_db,
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "add-exclude",
            "--pattern-type",
            "path_substring",
            "generated/",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Created excluded pattern:" in captured.out
    assert "pattern-type: path_substring" in captured.out
    assert "pattern-value: generated/" in captured.out
    assert "enabled: true" in captured.out

    session_factory = create_session_factory(cli_db)
    with rules_module.session_scope(session_factory) as session:
        created_pattern = RulesRepository(session).list_excluded_patterns()[0]

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "disable-exclude",
            str(created_pattern.id),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"Disabled excluded pattern: {created_pattern.id}" in captured.out
    assert "enabled: false" in captured.out


def test_rules_update_extension_requires_update_fields(
    cli_db,
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    session_factory = create_session_factory(cli_db)
    with rules_module.session_scope(session_factory) as session:
        RulesRepository(session).create_extension_rule(
            extension="aaf",
            enabled=True,
            max_size_bytes=100,
            oversize_action="skip",
            created_at="2026-03-15T10:00:00+00:00",
            updated_at="2026-03-15T10:00:00+00:00",
        )

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "update-extension",
            "aaf",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "At least one update field must be provided" in captured.err


def test_rules_disable_exclude_reports_missing_target(
    cli_db,
    monkeypatch,
    capsys,
) -> None:
    from backup_projects.cli import rules as rules_module

    _patch_rules_cli(monkeypatch=monkeypatch, rules_module=rules_module, engine=cli_db)

    exit_code = rules_module.main(
        [
            "--config",
            "config/app.yaml",
            "disable-exclude",
            "999",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Excluded pattern not found for id: 999" in captured.err


def _patch_rules_cli(*, monkeypatch, rules_module, engine) -> None:
    fake_config = SimpleNamespace()
    monkeypatch.setattr(
        rules_module,
        "load_config",
        lambda app_path, rules_path: fake_config,
    )
    monkeypatch.setattr(
        rules_module,
        "create_engine_from_config",
        lambda config: engine,
    )
