from __future__ import annotations

from flask import Flask, render_template
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.services.review_service import (
    build_manual_overrides_page_view,
    build_oversized_skipped_page_view,
    build_unrecognized_extensions_page_view,
)


def register_review_routes(
    app: Flask,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/review/oversized-skipped")
    def review_oversized_skipped():
        with session_scope(session_factory) as session:
            page_view = build_oversized_skipped_page_view(session=session)
        return render_template("review_oversized.html", review_page=page_view)

    @app.get("/review/unrecognized-extensions")
    def review_unrecognized_extensions():
        with session_scope(session_factory) as session:
            page_view = build_unrecognized_extensions_page_view(session=session)
        return render_template("review_unrecognized.html", review_page=page_view)

    @app.get("/review/manual-overrides")
    def review_manual_overrides():
        with session_scope(session_factory) as session:
            page_view = build_manual_overrides_page_view(session=session)
        return render_template("review_manual_overrides.html", review_page=page_view)
