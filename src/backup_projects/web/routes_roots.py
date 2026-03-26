from __future__ import annotations

from flask import Flask, render_template, request
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.services.roots_service import build_roots_page_view


def register_roots_routes(
    app: Flask,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/roots")
    def roots():
        with session_scope(session_factory) as session:
            roots_view = build_roots_page_view(
                session=session,
                status=request.args.get("status"),
                rescan=request.args.get("rescan"),
            )
        return render_template("roots.html", roots_page=roots_view)
