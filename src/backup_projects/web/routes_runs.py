from __future__ import annotations

from flask import Flask, abort, render_template, send_file
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.config import ProjectConfig
from backup_projects.services.runs_service import build_run_details_view, build_runs_history_view

REPORT_ARTIFACT_KEYS = frozenset({"json", "text", "html"})


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

    @app.get("/runs/<int:run_id>/reports/<artifact_key>")
    def download_run_report(run_id: int, artifact_key: str):
        if artifact_key not in REPORT_ARTIFACT_KEYS:
            abort(404)

        try:
            with session_scope(session_factory) as session:
                details_view = build_run_details_view(
                    session=session,
                    config=config,
                    run_id=run_id,
                )
        except LookupError:
            abort(404)

        artifact = next(
            (item for item in details_view.artifacts if item.key == artifact_key),
            None,
        )
        if artifact is None or not artifact.exists:
            abort(404)

        try:
            return send_file(artifact.path, as_attachment=True)
        except FileNotFoundError:
            abort(404)
