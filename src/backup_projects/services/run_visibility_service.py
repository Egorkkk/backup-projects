from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.repositories.runs_repo import (
    RunEventRecord,
    RunRecord,
    RunsRepository,
)
from backup_projects.services.logging_setup import build_run_log_path
from backup_projects.services.run_service import RunLifecycleEvent, RunLifecycleRecord


@dataclass(frozen=True, slots=True)
class ArtifactStatus:
    path: str
    exists: bool


@dataclass(frozen=True, slots=True)
class RunDetails:
    run: RunLifecycleRecord
    events: tuple[RunLifecycleEvent, ...]
    report_json: ArtifactStatus
    report_text: ArtifactStatus
    report_html: ArtifactStatus
    log_file: ArtifactStatus


def list_runs(
    *,
    session: Session,
    limit: int = 100,
) -> tuple[RunLifecycleRecord, ...]:
    repo = RunsRepository(session)
    return tuple(_to_run_lifecycle_record(record) for record in repo.list_runs(limit=limit))


def get_run_details(
    *,
    session: Session,
    run_id: int,
    reports_dir: str | Path,
    logs_dir: str | Path,
) -> RunDetails:
    repo = RunsRepository(session)
    run_record = repo.get_run(run_id)
    if run_record is None:
        raise LookupError(f"Run not found for id: {run_id}")

    report_dir = Path(reports_dir) / f"run-{run_id}"
    report_json_path = report_dir / "report.json"
    report_text_path = report_dir / "report.txt"
    report_html_path = report_dir / "report.html"
    log_file_path = build_run_log_path(logs_dir=logs_dir, run_id=run_id)

    return RunDetails(
        run=_to_run_lifecycle_record(run_record),
        events=tuple(_to_run_lifecycle_event(record) for record in repo.list_run_events(run_id)),
        report_json=_to_artifact_status(report_json_path),
        report_text=_to_artifact_status(report_text_path),
        report_html=_to_artifact_status(report_html_path),
        log_file=_to_artifact_status(log_file_path),
    )


def _to_artifact_status(path: Path) -> ArtifactStatus:
    return ArtifactStatus(path=str(path), exists=path.is_file())


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
