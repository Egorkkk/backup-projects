from __future__ import annotations

from flask import Flask, render_template
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.config import ProjectConfig
from backup_projects.services.actions_service import (
    ActionResult,
    dry_run_root_now,
    rescan_root_now,
    run_backup_now,
    run_daily_now,
)


def register_actions_routes(
    app: Flask,
    *,
    config: ProjectConfig,
    session_factory: sessionmaker[Session],
) -> None:
    @app.post("/actions/run-daily")
    def run_daily_action():
        try:
            with session_scope(session_factory) as session:
                result = run_daily_now(session=session, config=config)
        except Exception as exc:
            result = _build_failure_result(
                action_name="Run daily now",
                message=f"Daily run failed: {exc}",
                back_href="/",
                back_label="Back to dashboard",
            )
        return render_template("action_result.html", result=result)

    @app.post("/actions/run-backup")
    def run_backup_action():
        try:
            with session_scope(session_factory) as session:
                result = run_backup_now(session=session, config=config)
        except Exception as exc:
            result = _build_failure_result(
                action_name="Backup now",
                message=f"Backup run failed: {exc}",
                back_href="/",
                back_label="Back to dashboard",
            )
        return render_template("action_result.html", result=result)

    @app.post("/actions/roots/<int:root_id>/dry-run")
    def dry_run_root_action(root_id: int):
        try:
            with session_scope(session_factory) as session:
                result = dry_run_root_now(session=session, root_id=root_id)
        except Exception as exc:
            result = _build_failure_result(
                action_name="Dry-run now",
                message=f"Dry run failed for root {root_id}: {exc}",
                back_href="/roots",
                back_label="Back to roots",
            )
        return render_template("action_result.html", result=result)

    @app.post("/actions/roots/<int:root_id>/rescan")
    def rescan_root_action(root_id: int):
        try:
            with session_scope(session_factory) as session:
                result = rescan_root_now(session=session, config=config, root_id=root_id)
        except Exception as exc:
            result = _build_failure_result(
                action_name="Rescan root",
                message=f"Root rescan failed for root {root_id}: {exc}",
                back_href="/roots",
                back_label="Back to roots",
            )
        return render_template("action_result.html", result=result)


def _build_failure_result(
    *,
    action_name: str,
    message: str,
    back_href: str,
    back_label: str,
) -> ActionResult:
    return ActionResult(
        action_name=action_name,
        status="failed",
        message=message,
        back_href=back_href,
        back_label=back_label,
    )
