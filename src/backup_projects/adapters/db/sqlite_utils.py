from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, event

DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000


def resolve_sqlite_path(sqlite_path: str | Path, *, base_dir: str | Path | None = None) -> Path:
    path = Path(sqlite_path).expanduser()
    if path.is_absolute():
        return path.resolve()

    if base_dir is None:
        raise ValueError("base_dir is required for relative sqlite_path")

    return (Path(base_dir).expanduser().resolve() / path).resolve()


def ensure_sqlite_parent_dir(
    sqlite_path: str | Path, *, base_dir: str | Path | None = None
) -> Path:
    resolved_path = resolve_sqlite_path(sqlite_path, base_dir=base_dir)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_path


def make_sqlite_url(sqlite_path: str | Path, *, base_dir: str | Path | None = None) -> str:
    resolved_path = resolve_sqlite_path(sqlite_path, base_dir=base_dir)
    return f"sqlite+pysqlite:///{resolved_path.as_posix()}"


def apply_sqlite_pragmas(
    engine: Engine,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    enable_foreign_keys: bool = True,
) -> None:
    if busy_timeout_ms <= 0:
        raise ValueError("busy_timeout_ms must be > 0")

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            cursor.execute(f"PRAGMA foreign_keys = {1 if enable_foreign_keys else 0}")
        finally:
            cursor.close()
