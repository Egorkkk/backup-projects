from __future__ import annotations

from flask import Flask, render_template
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.config import ProjectConfig
from backup_projects.services.runs_service import build_run_details_view, build_runs_history_view


def register_runs_routes(
    app: Flask,
    *,
    config: ProjectConfig,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/runs")
    def runs():
        with session_scope(session_factory) as session:
            runs_view = build_runs_history_view(session=session)
        return render_template("runs.html", runs_page=runs_view)

    @app.get("/runs/<int:run_id>")
    def run_details(run_id: int):
        with session_scope(session_factory) as session:
            details_view = build_run_details_view(
                session=session,
                config=config,
                run_id=run_id,
            )
        return render_template("run_details.html", run_details=details_view)
