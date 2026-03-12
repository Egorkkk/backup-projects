from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase

metadata = MetaData()


class Base(DeclarativeBase):
    metadata = metadata


def create_schema(engine: Engine) -> None:
    with engine.begin() as connection:
        metadata.create_all(bind=connection)


def drop_schema(engine: Engine) -> None:
    with engine.begin() as connection:
        metadata.drop_all(bind=connection)
