from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from flask import render_template

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


def test_templates_render_identity_content_and_empty_states(tmp_path: Path) -> None:
    app = _build_test_app(tmp_path)
    dashboard = SimpleNamespace(
        last_scan=SimpleNamespace(
            run_id=1,
            run_type="daily",
            status="completed",
            started_at="2026-03-26T10:00:00+00:00",
            finished_at="2026-03-26T10:05:00+00:00",
            trigger_mode="manual",
        ),
        last_backup=SimpleNamespace(
            run_id=2,
            run_type="backup",
            status="completed",
            started_at="2026-03-26T10:10:00+00:00",
            finished_at="2026-03-26T10:15:00+00:00",
            trigger_mode="manual",
        ),
        run_status=SimpleNamespace(
            run_id=2,
            run_type="backup",
            status="completed",
            started_at="2026-03-26T10:10:00+00:00",
            finished_at="2026-03-26T10:15:00+00:00",
            trigger_mode="manual",
        ),
        counts=SimpleNamespace(included=10, skipped=2, new=1, changed=3),
        skipped_oversized_summary=SimpleNamespace(skipped_count=1, warning_count=1),
    )
    roots_page = SimpleNamespace(
        filters=SimpleNamespace(status="all", rescan="all"),
        rows=(),
    )
    dirs_page = SimpleNamespace(rows=())
    rules_page = SimpleNamespace(
        aaf_default_size_bytes=104857600,
        extension_rules=(),
        excluded_patterns=(),
    )
    includes_page = SimpleNamespace(
        available_roots=(SimpleNamespace(id=1, name="Show A"),),
        rows=(),
    )
    runs_page = SimpleNamespace(rows=())
    run_details = SimpleNamespace(
        id=7,
        run_type="backup",
        status="completed",
        trigger_mode="manual",
        started_at="2026-03-26T10:10:00+00:00",
        finished_at="2026-03-26T10:15:00+00:00",
        events=(),
        artifacts=(
            SimpleNamespace(key="json", label="report json", exists=True, path="/tmp/report.json"),
            SimpleNamespace(key="log", label="log file", exists=True, path="/tmp/run.log"),
        ),
    )
    result = ActionResult(
        action_name="Backup now",
        status="completed",
        message="Backup completed.",
        fields=(ActionResultField(label="Run id", value="5"),),
        details_text="summary",
        back_href="/",
        back_label="Back to dashboard",
    )
    review_empty = SimpleNamespace(rows=())

    with app.test_request_context("/"):
        assert "Dashboard" in render_template("dashboard.html", dashboard=dashboard)
        assert "Roots" in render_template("roots.html", roots_page=roots_page)
        assert "No roots found." in render_template("roots.html", roots_page=roots_page)
        assert "Project Dirs" in render_template("dirs.html", dirs_page=dirs_page)
        assert "No project dirs found." in render_template("dirs.html", dirs_page=dirs_page)
        assert "Rules" in render_template(
            "rules.html",
            rules_page=rules_page,
            error_message=None,
        )
        assert "No extension rules found." in render_template(
            "rules.html",
            rules_page=rules_page,
            error_message=None,
        )
        assert "Includes" in render_template(
            "includes.html",
            includes_page=includes_page,
            error_message=None,
        )
        assert "No manual includes found." in render_template(
            "includes.html",
            includes_page=includes_page,
            error_message=None,
        )
        assert "Runs" in render_template("runs.html", runs_page=runs_page)
        assert "No runs found." in render_template("runs.html", runs_page=runs_page)
        assert "Run Details" in render_template(
            "run_details.html",
            run_details=run_details,
        )
        run_details_html = render_template("run_details.html", run_details=run_details)
        assert "/runs/7/reports/json" in run_details_html
        assert "/runs/7/reports/log" not in run_details_html
        assert "Export report is not wired yet." not in run_details_html
        assert "Backup now" in render_template("action_result.html", result=result)
        assert "summary" in render_template("action_result.html", result=result)
        assert "Oversized Skipped Files" in render_template(
            "review_oversized.html",
            review_page=review_empty,
        )
        assert "No oversized skipped files found." in render_template(
            "review_oversized.html",
            review_page=review_empty,
        )
        assert "Unrecognized Extensions" in render_template(
            "review_unrecognized.html",
            review_page=review_empty,
        )
        assert "No unrecognized extensions found." in render_template(
            "review_unrecognized.html",
            review_page=review_empty,
        )
        assert "Manual Override Files" in render_template(
            "review_manual_overrides.html",
            review_page=review_empty,
        )
        assert "No manual override files found." in render_template(
            "review_manual_overrides.html",
            review_page=review_empty,
        )
