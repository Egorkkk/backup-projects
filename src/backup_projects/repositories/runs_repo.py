from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import run_events, runs


@dataclass(frozen=True)
class RunRecord:
    id: int
    run_type: str
    status: str
    started_at: str
    finished_at: str | None
    trigger_mode: str


@dataclass(frozen=True)
class RunEventRecord:
    id: int
    run_id: int
    event_time: str
    level: str
    event_type: str
    message: str
    payload_json: str | None


class RunsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create_run(
        self,
        *,
        run_type: str,
        status: str,
        started_at: str,
        trigger_mode: str,
        finished_at: str | None = None,
    ) -> RunRecord:
        result = self._session.execute(
            runs.insert().values(
                run_type=run_type,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                trigger_mode=trigger_mode,
            )
        )
        run_id = int(result.inserted_primary_key[0])
        record = self.get_run(run_id)
        if record is None:
            raise RuntimeError("Failed to load created run record")
        return record

    def get_run(self, run_id: int) -> RunRecord | None:
        row = (
            self._session.execute(select(runs).where(runs.c.id == run_id)).mappings().one_or_none()
        )
        if row is None:
            return None
        return _to_run_record(row)

    def list_runs(self, *, limit: int = 100) -> list[RunRecord]:
        rows = (
            self._session.execute(
                select(runs).order_by(runs.c.started_at.desc(), runs.c.id.desc()).limit(limit)
            )
            .mappings()
            .all()
        )
        return [_to_run_record(row) for row in rows]

    def update_run_status(
        self,
        run_id: int,
        *,
        status: str,
        finished_at: str | None = None,
    ) -> None:
        self._session.execute(
            update(runs).where(runs.c.id == run_id).values(status=status, finished_at=finished_at)
        )

    def add_run_event(
        self,
        *,
        run_id: int,
        event_time: str,
        level: str,
        event_type: str,
        message: str,
        payload_json: str | None = None,
    ) -> RunEventRecord:
        result = self._session.execute(
            run_events.insert().values(
                run_id=run_id,
                event_time=event_time,
                level=level,
                event_type=event_type,
                message=message,
                payload_json=payload_json,
            )
        )
        event_id = int(result.inserted_primary_key[0])
        row = (
            self._session.execute(select(run_events).where(run_events.c.id == event_id))
            .mappings()
            .one()
        )
        return _to_run_event_record(row)

    def list_run_events(self, run_id: int) -> list[RunEventRecord]:
        rows = (
            self._session.execute(
                select(run_events)
                .where(run_events.c.run_id == run_id)
                .order_by(run_events.c.event_time, run_events.c.id)
            )
            .mappings()
            .all()
        )
        return [_to_run_event_record(row) for row in rows]


def _to_run_record(row) -> RunRecord:
    return RunRecord(
        id=row["id"],
        run_type=row["run_type"],
        status=row["status"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        trigger_mode=row["trigger_mode"],
    )


def _to_run_event_record(row) -> RunEventRecord:
    return RunEventRecord(
        id=row["id"],
        run_id=row["run_id"],
        event_time=row["event_time"],
        level=row["level"],
        event_type=row["event_type"],
        message=row["message"],
        payload_json=row["payload_json"],
    )
