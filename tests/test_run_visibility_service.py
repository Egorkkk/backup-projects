from datetime import datetime, timezone
from pathlib import Path

import pytest

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.services.run_service import append_run_event, finish_run, start_run
from backup_projects.services.run_visibility_service import get_run_details, list_runs


@pytest.fixture
def session_factory(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "run-visibility.sqlite3")
    create_schema(engine)
    factory = create_session_factory(engine)
    try:
        yield factory
    finally:
        engine.dispose()


def test_list_runs_returns_compact_lifecycle_rows_only(session_factory) -> None:
    with session_scope(session_factory) as session:
        first_run = start_run(
            session=session,
            run_type="daily",
            trigger_mode="cron",
            now=lambda: datetime(2026, 3, 17, 9, 0, tzinfo=timezone.utc),
        )
        finish_run(
            session=session,
            run_id=first_run.id,
            status="completed",
            now=lambda: datetime(2026, 3, 17, 9, 5, tzinfo=timezone.utc),
        )
        start_run(
            session=session,
            run_type="backup",
            trigger_mode="manual",
            now=lambda: datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc),
        )

        runs = list_runs(session=session, limit=1)

    assert len(runs) == 1
    assert runs[0].run_type == "backup"
    assert runs[0].status == "running"
    assert runs[0].trigger_mode == "manual"


def test_get_run_details_returns_run_events_and_artifact_statuses(
    tmp_path: Path,
    session_factory,
) -> None:
    reports_dir = tmp_path / "runtime" / "reports"
    logs_dir = tmp_path / "runtime" / "logs"
    (reports_dir / "run-1").mkdir(parents=True)
    (logs_dir / "run-1").mkdir(parents=True)
    (reports_dir / "run-1" / "report.json").write_text("{not-json", encoding="utf-8")
    (reports_dir / "run-1" / "report.txt").write_text("ignored", encoding="utf-8")
    (logs_dir / "run-1" / "run.log").write_text("\x00raw", encoding="utf-8")

    with session_scope(session_factory) as session:
        run = start_run(
            session=session,
            run_type="daily",
            trigger_mode="cron",
            now=lambda: datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc),
        )
        append_run_event(
            session=session,
            run_id=run.id,
            event_type="manifest_built",
            message="Manifest built",
            payload={"included_count": 3},
            now=lambda: datetime(2026, 3, 17, 10, 1, tzinfo=timezone.utc),
        )

        details = get_run_details(
            session=session,
            run_id=run.id,
            reports_dir=reports_dir,
            logs_dir=logs_dir,
        )

    assert details.run.id == 1
    assert details.run.run_type == "daily"
    assert len(details.events) == 1
    assert details.events[0].event_type == "manifest_built"
    assert details.events[0].payload == {"included_count": 3}
    assert details.report_json.path.endswith("run-1/report.json")
    assert details.report_json.exists is True
    assert details.report_text.path.endswith("run-1/report.txt")
    assert details.report_text.exists is True
    assert details.report_html.path.endswith("run-1/report.html")
    assert details.report_html.exists is False
    assert details.log_file.path.endswith("run-1/run.log")
    assert details.log_file.exists is True


def test_get_run_details_raises_lookup_error_for_unknown_run(session_factory) -> None:
    with session_scope(session_factory) as session:
        with pytest.raises(LookupError, match="^Run not found for id: 999$"):
            get_run_details(
                session=session,
                run_id=999,
                reports_dir="/tmp/reports",
                logs_dir="/tmp/logs",
            )
