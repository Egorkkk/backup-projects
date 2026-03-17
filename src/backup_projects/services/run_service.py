from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from backup_projects.repositories.runs_repo import (
    RunEventRecord,
    RunRecord,
    RunsRepository,
)


@dataclass(frozen=True, slots=True)
class RunLifecycleRecord:
    id: int
    run_type: str
    status: str
    started_at: str
    trigger_mode: str
    finished_at: str | None = None


@dataclass(frozen=True, slots=True)
class RunLifecycleEvent:
    id: int
    run_id: int
    event_time: str
    level: str
    event_type: str
    message: str
    payload: dict[str, object] | None = None


def start_run(
    *,
    session: Session,
    run_type: str,
    trigger_mode: str,
    now: Callable[[], datetime] | None = None,
) -> RunLifecycleRecord:
    repo = RunsRepository(session)
    run_record = repo.create_run(
        run_type=run_type,
        status="running",
        started_at=_format_timestamp(_resolve_now(now)),
        trigger_mode=trigger_mode,
    )
    return _to_run_lifecycle_record(run_record)


def append_run_event(
    *,
    session: Session,
    run_id: int,
    event_type: str,
    message: str,
    level: str = "INFO",
    payload: Mapping[str, object] | None = None,
    now: Callable[[], datetime] | None = None,
) -> RunLifecycleEvent:
    repo = RunsRepository(session)
    run_record = repo.get_run(run_id)
    if run_record is None:
        raise LookupError(f"Run not found for id: {run_id}")
    event_record = repo.add_run_event(
        run_id=run_id,
        event_time=_format_timestamp(_resolve_now(now)),
        level=level,
        event_type=event_type,
        message=message,
        payload_json=_serialize_payload(payload),
    )
    return _to_run_lifecycle_event(event_record)


def finish_run(
    *,
    session: Session,
    run_id: int,
    status: str,
    now: Callable[[], datetime] | None = None,
) -> RunLifecycleRecord:
    repo = RunsRepository(session)
    run_record = repo.get_run(run_id)
    if run_record is None:
        raise LookupError(f"Run not found for id: {run_id}")
    if run_record.finished_at is not None:
        raise ValueError(f"Run is already finished: {run_id}")
    finished_at = _format_timestamp(_resolve_now(now))
    repo.update_run_status(
        run_id,
        status=status,
        finished_at=finished_at,
    )
    run_record = repo.get_run(run_id)
    if run_record is None:
        raise LookupError(f"Run not found for id: {run_id}")
    return _to_run_lifecycle_record(run_record)


def _resolve_now(now: Callable[[], datetime] | None) -> datetime:
    return _utc_now() if now is None else now()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _serialize_payload(payload: Mapping[str, object] | None) -> str | None:
    if payload is None:
        return None
    return json.dumps(dict(payload), sort_keys=True)


def _to_run_lifecycle_record(record: RunRecord) -> RunLifecycleRecord:
    return RunLifecycleRecord(
        id=record.id,
        run_type=record.run_type,
        status=record.status,
        started_at=record.started_at,
        trigger_mode=record.trigger_mode,
        finished_at=record.finished_at,
    )


def _to_run_lifecycle_event(record: RunEventRecord) -> RunLifecycleEvent:
    payload = json.loads(record.payload_json) if record.payload_json is not None else None
    return RunLifecycleEvent(
        id=record.id,
        run_id=record.run_id,
        event_time=record.event_time,
        level=record.level,
        event_type=record.event_type,
        message=record.message,
        payload=payload,
    )
