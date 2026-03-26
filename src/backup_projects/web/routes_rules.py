from __future__ import annotations

from flask import Flask, redirect, render_template, request, url_for
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.session import session_scope
from backup_projects.services.rules_service import (
    build_rules_page_view,
    create_excluded_pattern,
    create_extension_rule,
    toggle_excluded_pattern,
    update_extension_rule,
)


def register_rules_routes(
    app: Flask,
    *,
    session_factory: sessionmaker[Session],
) -> None:
    @app.get("/rules")
    def rules():
        with session_scope(session_factory) as session:
            rules_view = build_rules_page_view(session=session)
        return render_template("rules.html", rules_page=rules_view, error_message=None)

    @app.post("/rules/extensions")
    def create_extension_rule_handler():
        try:
            with session_scope(session_factory) as session:
                create_extension_rule(
                    session=session,
                    extension=request.form.get("extension", ""),
                    enabled=request.form.get("enabled") == "true",
                    max_size_bytes_raw=request.form.get("max_size_bytes"),
                    oversize_action=request.form.get("oversize_action", ""),
                )
        except (LookupError, ValueError) as exc:
            return _render_rules_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("rules"))

    @app.post("/rules/extensions/<extension>/update")
    def update_extension_rule_handler(extension: str):
        try:
            with session_scope(session_factory) as session:
                update_extension_rule(
                    session=session,
                    extension=extension,
                    enabled=request.form.get("enabled") == "true",
                    max_size_bytes_raw=request.form.get("max_size_bytes"),
                    clear_max_size=request.form.get("clear_max_size") == "true",
                    oversize_action=request.form.get("oversize_action", ""),
                )
        except (LookupError, ValueError) as exc:
            return _render_rules_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("rules"))

    @app.post("/rules/excludes")
    def create_excluded_pattern_handler():
        try:
            with session_scope(session_factory) as session:
                create_excluded_pattern(
                    session=session,
                    pattern_type=request.form.get("pattern_type", ""),
                    pattern_value=request.form.get("pattern_value", ""),
                    enabled=request.form.get("enabled") == "true",
                )
        except (LookupError, ValueError) as exc:
            return _render_rules_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("rules"))

    @app.post("/rules/excludes/<int:pattern_id>/toggle")
    def toggle_excluded_pattern_handler(pattern_id: int):
        try:
            with session_scope(session_factory) as session:
                toggle_excluded_pattern(session=session, pattern_id=pattern_id)
        except (LookupError, ValueError) as exc:
            return _render_rules_page_with_error(
                session_factory=session_factory,
                error_message=str(exc),
            )
        return redirect(url_for("rules"))


def _render_rules_page_with_error(
    *,
    session_factory: sessionmaker[Session],
    error_message: str,
):
    with session_scope(session_factory) as session:
        rules_view = build_rules_page_view(session=session)
    return render_template("rules.html", rules_page=rules_view, error_message=error_message), 400
