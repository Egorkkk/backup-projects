from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.repositories.runs_repo import RunsRepository
from backup_projects.services.run_service import (
    append_run_event,
    finish_run,
    start_run,
)


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "run-service.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_start_run_creates_run_with_running_status(db_session: Session) -> None:
    started_at = datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc)

    result = start_run(
        session=db_session,
        run_type="daily",
        trigger_mode="cron",
        now=lambda: started_at,
    )

    stored_run = RunsRepository(db_session).get_run(result.id)

    assert result.status == "running"
    assert result.started_at == "2026-03-17T10:00:00+00:00"
    assert result.run_type == "daily"
    assert result.trigger_mode == "cron"
    assert result.finished_at is None
    assert stored_run == RunsRepository(db_session).get_run(result.id)
    assert stored_run is not None
    assert stored_run.status == "running"
    assert stored_run.started_at == "2026-03-17T10:00:00+00:00"
    assert stored_run.run_type == "daily"
    assert stored_run.trigger_mode == "cron"


def test_append_run_event_persists_domain_event_payload(db_session: Session) -> None:
    run = start_run(
        session=db_session,
        run_type="backup",
        trigger_mode="manual",
        now=lambda: datetime(2026, 3, 17, 11, 0, tzinfo=timezone.utc),
    )

    event = append_run_event(
        session=db_session,
        run_id=run.id,
        event_type="manifest_built",
        message="Manifest built",
        payload={"included_count": 3, "warning_count": 1},
        now=lambda: datetime(2026, 3, 17, 11, 1, tzinfo=timezone.utc),
    )

    stored_events = RunsRepository(db_session).list_run_events(run.id)

    assert event.run_id == run.id
    assert event.event_type == "manifest_built"
    assert event.message == "Manifest built"
    assert event.level == "INFO"
    assert event.event_time == "2026-03-17T11:01:00+00:00"
    assert event.payload == {"included_count": 3, "warning_count": 1}
    assert len(stored_events) == 1
    assert stored_events[0].payload_json == '{"included_count": 3, "warning_count": 1}'


def test_append_run_event_raises_lookup_error_for_unknown_run_id(db_session: Session) -> None:
    with pytest.raises(LookupError, match="^Run not found for id: 9999$"):
        append_run_event(
            session=db_session,
            run_id=9999,
            event_type="manifest_built",
            message="Manifest built",
            now=lambda: datetime(2026, 3, 17, 11, 1, tzinfo=timezone.utc),
        )


def test_finish_run_updates_status_and_finished_at(db_session: Session) -> None:
    run = start_run(
        session=db_session,
        run_type="weekly",
        trigger_mode="manual",
        now=lambda: datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc),
    )

    result = finish_run(
        session=db_session,
        run_id=run.id,
        status="completed",
        now=lambda: datetime(2026, 3, 17, 12, 5, tzinfo=timezone.utc),
    )

    stored_run = RunsRepository(db_session).get_run(run.id)

    assert result.id == run.id
    assert result.status == "completed"
    assert result.finished_at == "2026-03-17T12:05:00+00:00"
    assert stored_run is not None
    assert stored_run.status == "completed"
    assert stored_run.finished_at == "2026-03-17T12:05:00+00:00"


def test_finish_run_rejects_repeated_finish_and_keeps_original_state(
    db_session: Session,
) -> None:
    run = start_run(
        session=db_session,
        run_type="weekly",
        trigger_mode="manual",
        now=lambda: datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc),
    )

    finish_run(
        session=db_session,
        run_id=run.id,
        status="completed",
        now=lambda: datetime(2026, 3, 17, 12, 5, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError, match="^Run is already finished: 1$"):
        finish_run(
            session=db_session,
            run_id=run.id,
            status="failed",
            now=lambda: datetime(2026, 3, 17, 12, 10, tzinfo=timezone.utc),
        )

    stored_run = RunsRepository(db_session).get_run(run.id)

    assert stored_run is not None
    assert stored_run.status == "completed"
    assert stored_run.finished_at == "2026-03-17T12:05:00+00:00"
