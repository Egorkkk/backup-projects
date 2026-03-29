from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

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


def _dashboard_view():
    metric = SimpleNamespace(
        run_id=11,
        run_type="daily",
        status="completed",
        started_at="2026-03-26T10:00:00+00:00",
        finished_at="2026-03-26T10:05:00+00:00",
        trigger_mode="manual",
    )
    return SimpleNamespace(
        last_scan=metric,
        last_backup=metric,
        run_status=metric,
        counts=SimpleNamespace(included=3, skipped=1, new=2, changed=1),
        skipped_oversized_summary=SimpleNamespace(skipped_count=1, warning_count=1),
    )


@pytest.mark.parametrize(
    ("route", "patch_target", "return_value", "expected_text"),
    [
        (
            "/",
            "backup_projects.web.routes_dashboard.build_dashboard_view",
            _dashboard_view(),
            "Dashboard",
        ),
        (
            "/roots",
            "backup_projects.web.routes_roots.build_roots_page_view",
            SimpleNamespace(
                filters=SimpleNamespace(status="all", rescan="all"),
                rows=(
                    SimpleNamespace(
                        id=1,
                        raid_name="raid-a",
                        name="Show A",
                        path="/raid-a/show-a",
                        status="active",
                        needs_structural_rescan=False,
                        last_seen_at="2026-03-26T10:00:00+00:00",
                    ),
                ),
            ),
            "Roots",
        ),
        (
            "/dirs",
            "backup_projects.web.routes_dirs.build_project_dirs_page_view",
            SimpleNamespace(
                rows=(
                    SimpleNamespace(
                        id=1,
                        root_id=1,
                        root_name="Show A",
                        root_path="/raid-a/show-a",
                        relative_path="edit/project",
                        name="project",
                        dir_type="premiere",
                        status="active",
                        last_seen_at="2026-03-26T10:00:00+00:00",
                    ),
                ),
            ),
            "Project Dirs",
        ),
        (
            "/rules",
            "backup_projects.web.routes_rules.build_rules_page_view",
            SimpleNamespace(
                aaf_default_size_bytes=104857600,
                extension_rules=(
                    SimpleNamespace(
                        id=1,
                        extension="aaf",
                        enabled=True,
                        max_size_bytes=104857600,
                        oversize_action="warn",
                    ),
                ),
                excluded_patterns=(
                    SimpleNamespace(
                        id=1,
                        pattern_type="glob",
                        pattern_value="*.tmp",
                        enabled=True,
                    ),
                ),
            ),
            "Rules",
        ),
        (
            "/includes",
            "backup_projects.web.routes_includes.build_includes_page_view",
            SimpleNamespace(
                available_roots=(SimpleNamespace(id=1, name="Show A"),),
                rows=(
                    SimpleNamespace(
                        id=1,
                        root_id=1,
                        root_name="Show A",
                        relative_path="manual/file.prproj",
                        include_path_type="file",
                        recursive=False,
                        force_include=True,
                        enabled=True,
                        updated_at="2026-03-26T10:00:00+00:00",
                    ),
                ),
            ),
            "Includes",
        ),
        (
            "/runs",
            "backup_projects.web.routes_runs.build_runs_history_view",
            SimpleNamespace(
                rows=(
                    SimpleNamespace(
                        id=7,
                        run_type="backup",
                        status="completed",
                        trigger_mode="manual",
                        started_at="2026-03-26T10:00:00+00:00",
                        finished_at="2026-03-26T10:05:00+00:00",
                    ),
                ),
            ),
            "Runs",
        ),
        (
            "/runs/7",
            "backup_projects.web.routes_runs.build_run_details_view",
            SimpleNamespace(
                id=7,
                run_type="backup",
                status="completed",
                trigger_mode="manual",
                started_at="2026-03-26T10:00:00+00:00",
                finished_at="2026-03-26T10:05:00+00:00",
                events=(
                    SimpleNamespace(
                        event_time="2026-03-26T10:01:00+00:00",
                        level="INFO",
                        event_type="backup_started",
                        message="Backup started",
                    ),
                ),
                artifacts=(
                    SimpleNamespace(
                        key="json",
                        label="report json",
                        exists=True,
                        path="/tmp/report.json",
                    ),
                ),
            ),
            "Run Details",
        ),
        (
            "/review/oversized-skipped",
            "backup_projects.web.routes_review.build_oversized_skipped_page_view",
            SimpleNamespace(
                rows=(
                    SimpleNamespace(
                        root_id=1,
                        root_path="/raid-a/show-a",
                        file_path="/raid-a/show-a/edit/file.aaf",
                        extension="aaf",
                        size_bytes=104857601,
                        oversize_action="skip",
                        warning="oversize",
                    ),
                ),
            ),
            "Oversized Skipped Files",
        ),
        (
            "/review/unrecognized-extensions",
            "backup_projects.web.routes_review.build_unrecognized_extensions_page_view",
            SimpleNamespace(
                rows=(
                    SimpleNamespace(
                        extension="xyz",
                        occurrence_count=2,
                        sample_path="/raid-a/show-a/edit/file.xyz",
                    ),
                ),
            ),
            "Unrecognized Extensions",
        ),
        (
            "/review/manual-overrides",
            "backup_projects.web.routes_review.build_manual_overrides_page_view",
            SimpleNamespace(
                rows=(
                    SimpleNamespace(
                        root_id=1,
                        root_path="/raid-a/show-a",
                        file_path="/raid-a/show-a/edit/file.mov",
                        extension="mov",
                        size_bytes=123,
                        reason="force_include_override_policy_oversize",
                        manual_include_applied=True,
                        force_include_applied=True,
                    ),
                ),
            ),
            "Manual Override Files",
        ),
    ],
)
def test_basic_get_routes_render_expected_page_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    patch_target: str,
    return_value: object,
    expected_text: str,
) -> None:
    app = _build_test_app(tmp_path)
    monkeypatch.setattr(patch_target, lambda **kwargs: return_value)

    response = app.test_client().get(route)

    assert response.status_code == 200
    assert expected_text in response.get_data(as_text=True)


def test_runs_report_download_returns_attachment_for_existing_report_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _build_test_app(tmp_path)
    report_path = tmp_path / "runtime" / "reports" / "run-7" / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text('{"status":"ok"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "backup_projects.web.routes_runs.build_run_details_view",
        lambda **kwargs: SimpleNamespace(
            id=7,
            artifacts=(
                SimpleNamespace(key="json", label="report json", exists=True, path=str(report_path)),
            ),
        ),
    )

    response = app.test_client().get("/runs/7/reports/json")

    assert response.status_code == 200
    assert response.headers["Content-Disposition"].startswith("attachment;")
    assert response.get_data(as_text=True) == '{"status":"ok"}\n'


def test_runs_report_download_returns_404_for_invalid_artifact_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _build_test_app(tmp_path)
    mocked_view = Mock()
    monkeypatch.setattr("backup_projects.web.routes_runs.build_run_details_view", mocked_view)

    response = app.test_client().get("/runs/7/reports/log")

    assert response.status_code == 404
    mocked_view.assert_not_called()


def test_runs_report_download_returns_404_when_report_file_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _build_test_app(tmp_path)
    missing_path = tmp_path / "runtime" / "reports" / "run-7" / "report.html"
    monkeypatch.setattr(
        "backup_projects.web.routes_runs.build_run_details_view",
        lambda **kwargs: SimpleNamespace(
            id=7,
            artifacts=(
                SimpleNamespace(
                    key="html",
                    label="report html",
                    exists=False,
                    path=str(missing_path),
                ),
            ),
        ),
    )

    response = app.test_client().get("/runs/7/reports/html")

    assert response.status_code == 404


def test_runs_report_download_returns_404_for_unknown_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _build_test_app(tmp_path)
    monkeypatch.setattr(
        "backup_projects.web.routes_runs.build_run_details_view",
        lambda **kwargs: (_ for _ in ()).throw(LookupError("Run not found for id: 999")),
    )

    response = app.test_client().get("/runs/999/reports/json")

    assert response.status_code == 404
