from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from backup_projects.adapters.db.sqlite_utils import (
    apply_sqlite_pragmas,
    ensure_sqlite_parent_dir,
    make_sqlite_url,
)
from backup_projects.config import DbSettings, ProjectConfig


def create_sqlite_engine(
    sqlite_path: str | Path,
    *,
    echo: bool = False,
    base_dir: str | Path | None = None,
) -> Engine:
    resolved_path = ensure_sqlite_parent_dir(sqlite_path, base_dir=base_dir)
    engine = create_engine(make_sqlite_url(resolved_path), echo=echo, future=True)
    apply_sqlite_pragmas(engine)
    return engine


def create_engine_from_db_settings(
    db_settings: DbSettings,
    *,
    echo: bool = False,
    base_dir: str | Path | None = None,
) -> Engine:
    if db_settings.driver != "sqlite":
        raise ValueError(f"Unsupported db driver: {db_settings.driver}")
    return create_sqlite_engine(db_settings.sqlite_path, echo=echo, base_dir=base_dir)


def create_engine_from_config(config: ProjectConfig, *, echo: bool = False) -> Engine:
    return create_engine_from_db_settings(
        config.app_config.db,
        echo=echo,
        base_dir=config.app_path.parent,
    )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def connection_scope(engine: Engine) -> Iterator[Connection]:
    with engine.begin() as connection:
        yield connection
