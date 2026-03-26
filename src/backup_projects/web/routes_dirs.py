from __future__ import annotations

from flask import Flask, render_template
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.services.project_dirs_service import build_project_dirs_page_view


def register_dirs_routes(
    app: Flask,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/dirs")
    def dirs():
        with session_scope(session_factory) as session:
            dirs_view = build_project_dirs_page_view(session=session)
        return render_template("dirs.html", dirs_page=dirs_view)
