from __future__ import annotations

from pathlib import Path

from flask import Flask

from backup_projects.adapters.db.session import create_engine_from_config, create_session_factory
from backup_projects.config import ProjectConfig, load_config
from backup_projects.web.routes_dashboard import register_dashboard_routes
from backup_projects.web.routes_dirs import register_dirs_routes
from backup_projects.web.routes_roots import register_roots_routes
from backup_projects.web.routes_rules import register_rules_routes


def create_app(
    *,
    config: ProjectConfig | None = None,
    app_path: str | Path = "config/app.yaml",
    rules_path: str | Path = "config/rules.yaml",
) -> Flask:
    project_config = load_config(app_path=app_path, rules_path=rules_path) if config is None else config

    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    engine = create_engine_from_config(project_config)
    session_factory = create_session_factory(engine)

    register_dashboard_routes(
        app,
        config=project_config,
        session_factory=session_factory,
    )
    register_roots_routes(
        app,
        session_factory=session_factory,
    )
    register_dirs_routes(
        app,
        session_factory=session_factory,
    )
    register_rules_routes(
        app,
        session_factory=session_factory,
    )

    return app
