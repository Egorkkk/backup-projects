from __future__ import annotations

from flask import Flask, redirect, render_template, request, url_for
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.services.includes_service import (
    build_includes_page_view,
    create_include,
    delete_include,
    toggle_include_enabled,
)


def register_includes_routes(
    app: Flask,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/includes")
    def includes():
        with session_scope(session_factory) as session:
            includes_view = build_includes_page_view(session=session)
        return render_template(
            "includes.html",
            includes_page=includes_view,
            error_message=None,
        )

    @app.post("/includes")
    def create_include_handler():
        try:
            with session_scope(session_factory) as session:
                create_include(
                    session=session,
                    root_id_raw=request.form.get("root_id", ""),
                    target_path=request.form.get("target_path", ""),
                    include_path_type=request.form.get("include_path_type", ""),
                    recursive=request.form.get("recursive") == "true",
                    force_include=request.form.get("force_include") == "true",
                    enabled=request.form.get("enabled") == "true",
                )
        except (LookupError, ValueError, FileNotFoundError, NotADirectoryError) as exc:
            return _render_includes_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("includes"))

    @app.post("/includes/<int:include_id>/toggle")
    def toggle_include_handler(include_id: int):
        try:
            with session_scope(session_factory) as session:
                toggle_include_enabled(session=session, include_id=include_id)
        except (LookupError, ValueError) as exc:
            return _render_includes_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("includes"))

    @app.post("/includes/<int:include_id>/delete")
    def delete_include_handler(include_id: int):
        try:
            with session_scope(session_factory) as session:
                delete_include(session=session, include_id=include_id)
        except (LookupError, ValueError) as exc:
            return _render_includes_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("includes"))


def _render_includes_page_with_error(
    *,
    session_factory: sessionmaker[Session],
    error_message: str,
):
    with session_scope(session_factory) as session:
        includes_view = build_includes_page_view(session=session)
    return (
        render_template(
            "includes.html",
            includes_page=includes_view,
            error_message=error_message,
        ),
        400,
    )
