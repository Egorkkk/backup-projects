from __future__ import annotations

from flask import Flask, render_template
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.config import ProjectConfig
from backup_projects.services.dashboard_service import build_dashboard_view


def register_dashboard_routes(
    app: Flask,
    *,
    config: ProjectConfig,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/")
    def dashboard():
        with session_scope(session_factory) as session:
            dashboard_view = build_dashboard_view(
                session=session,
                config=config,
            )
        return render_template(
            "dashboard.html",
            dashboard=dashboard_view,
        )
