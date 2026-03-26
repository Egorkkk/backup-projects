from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from backup_projects.services.actions_service import ActionResult, ActionResultField
from backup_projects.web.app import create_app


def _build_test_app(tmp_path: Path):
    app_config_path = tmp_path / "config" / "app.yaml"
    rules_config_path = tmp_path / "config" / "rules.yaml"
    app_config_path.parent.mkdir(parents=True, exist_ok=True)

    app_config_path.write_text(
        "\n".join(
            [
                "app:",
                "  name: test-app",
                "  env: test",
                "  log_level: INFO",
                "raid_roots:",
                "  - name: raid-a",
                f"    path: {tmp_path / 'raid-a'}",
                "    enabled: true",
                "runtime:",
                "  logs_dir: runtime/logs",
                "  manifests_dir: runtime/manifests",
                "  reports_dir: runtime/reports",
                "  db_dir: runtime/db",
                "  locks_dir: runtime/locks",
                "web:",
                "  host: 127.0.0.1",
                "  port: 5000",
                "  debug: false",
                "db:",
                "  driver: sqlite",
                "  sqlite_path: runtime/db/app.sqlite3",
                "restic:",
                "  binary: restic",
                "  repository: /tmp/restic-repo",
                "  password_env_var: RESTIC_PASSWORD",
                "  timeout_seconds: 60",
                "scheduler:",
                "  mode: cron",
                "",
            ]
        ),
        encoding="utf-8",
    )
    rules_config_path.write_text(
        "\n".join(
            [
                "allowed_extensions:",
                "  - prproj",
                "  - aaf",
                "size_limits:",
                "  default_max_size_bytes: null",
                "  by_extension:",
                "    aaf: 104857600",
                "oversize:",
                "  default_action: skip",
                "  aaf_action: warn",
                "  log_skipped: true",
                "exclude_patterns:",
                "  directory_names: []",
                "  glob_patterns: []",
                "  path_substrings: []",
                "unknown_extensions:",
                "  action: collect_and_skip",
                "  store_in_registry: true",
                "  log_warning: true",
                "",
            ]
        ),
        encoding="utf-8",
    )

    app = create_app(app_path=app_config_path, rules_path=rules_config_path)
    app.config.update(TESTING=True)
    return app


def _action_result(*, action_name: str, status: str, message: str) -> ActionResult:
    return ActionResult(
        action_name=action_name,
        status=status,
        message=message,
        fields=(ActionResultField(label="Run id", value="41"),),
        back_href="/",
        back_label="Back",
    )


@pytest.mark.parametrize(
    ("route", "patch_name", "result", "expected_text"),
    [
        (
            "/actions/run-daily",
            "run_daily_now",
            _action_result(
                action_name="Run daily now",
                status="completed",
                message="Daily completed",
            ),
            "Daily completed",
        ),
        (
            "/actions/run-backup",
            "run_backup_now",
            _action_result(
                action_name="Backup now",
                status="locked",
                message="Backup locked",
            ),
            "Backup locked",
        ),
    ],
)
def test_global_action_routes_render_action_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    patch_name: str,
    result: ActionResult,
    expected_text: str,
) -> None:
    app = _build_test_app(tmp_path)
    mocked_action = Mock(return_value=result)
    monkeypatch.setattr(f"backup_projects.web.routes_actions.{patch_name}", mocked_action)

    response = app.test_client().post(route)

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert expected_text in body
    mocked_action.assert_called_once()


@pytest.mark.parametrize(
    ("route", "patch_name", "expected_text"),
    [
        (
            "/actions/roots/7/dry-run",
            "dry_run_root_now",
            "Dry run complete",
        ),
        (
            "/actions/roots/9/rescan",
            "rescan_root_now",
            "Rescan locked",
        ),
    ],
)
def test_root_scoped_action_routes_pass_root_id_and_render_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    patch_name: str,
    expected_text: str,
) -> None:
    app = _build_test_app(tmp_path)
    mocked_action = Mock(
        return_value=_action_result(
            action_name="Root action",
            status="completed",
            message=expected_text,
        )
    )
    monkeypatch.setattr(f"backup_projects.web.routes_actions.{patch_name}", mocked_action)

    response = app.test_client().post(route)

    assert response.status_code == 200
    assert expected_text in response.get_data(as_text=True)
    mocked_action.assert_called_once()
    assert mocked_action.call_args.kwargs["root_id"] == int(route.split("/")[3])


@pytest.mark.parametrize(
    ("route", "patch_name", "expected_text"),
    [
        (
            "/actions/run-daily",
            "run_daily_now",
            "Daily run failed: boom",
        ),
        (
            "/actions/roots/5/rescan",
            "rescan_root_now",
            "Root rescan failed for root 5: boom",
        ),
    ],
)
def test_action_routes_render_failure_page_when_action_service_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    patch_name: str,
    expected_text: str,
) -> None:
    app = _build_test_app(tmp_path)
    mocked_action = Mock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(f"backup_projects.web.routes_actions.{patch_name}", mocked_action)

    response = app.test_client().post(route)

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Status: failed" in body
    assert expected_text in body
    mocked_action.assert_called_once()
