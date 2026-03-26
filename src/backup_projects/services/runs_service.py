from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ProjectConfig
from backup_projects.services.run_visibility_service import get_run_details, list_runs


@dataclass(frozen=True, slots=True)
class RunHistoryRow:
    id: int
    run_type: str
    status: str
    trigger_mode: str
    started_at: str
    finished_at: str | None


@dataclass(frozen=True, slots=True)
class RunsHistoryView:
    rows: tuple[RunHistoryRow, ...]


@dataclass(frozen=True, slots=True)
class RunEventRow:
    event_time: str
    level: str
    event_type: str
    message: str


@dataclass(frozen=True, slots=True)
class RunArtifactRow:
    label: str
    path: str
    exists: bool


@dataclass(frozen=True, slots=True)
class RunDetailsView:
    id: int
    run_type: str
    status: str
    trigger_mode: str
    started_at: str
    finished_at: str | None
    events: tuple[RunEventRow, ...]
    artifacts: tuple[RunArtifactRow, ...]


def build_runs_history_view(*, session: Session) -> RunsHistoryView:
    runs = list_runs(session=session, limit=100)
    return RunsHistoryView(
        rows=tuple(
            RunHistoryRow(
                id=run.id,
                run_type=run.run_type,
                status=run.status,
                trigger_mode=run.trigger_mode,
                started_at=run.started_at,
                finished_at=run.finished_at,
            )
            for run in runs
        )
    )


def build_run_details_view(
    *,
    session: Session,
    config: ProjectConfig,
    run_id: int,
) -> RunDetailsView:
    details = get_run_details(
        session=session,
        run_id=run_id,
        reports_dir=_resolve_runtime_dir(config, config.app_config.runtime.reports_dir),
        logs_dir=_resolve_runtime_dir(config, config.app_config.runtime.logs_dir),
    )

    return RunDetailsView(
        id=details.run.id,
        run_type=details.run.run_type,
        status=details.run.status,
        trigger_mode=details.run.trigger_mode,
        started_at=details.run.started_at,
        finished_at=details.run.finished_at,
        events=tuple(
            RunEventRow(
                event_time=event.event_time,
                level=event.level,
                event_type=event.event_type,
                message=event.message,
            )
            for event in details.events
        ),
        artifacts=(
            RunArtifactRow(
                label="report json",
                path=details.report_json.path,
                exists=details.report_json.exists,
            ),
            RunArtifactRow(
                label="report text",
                path=details.report_text.path,
                exists=details.report_text.exists,
            ),
            RunArtifactRow(
                label="report html",
                path=details.report_html.path,
                exists=details.report_html.exists,
            ),
            RunArtifactRow(
                label="log file",
                path=details.log_file.path,
                exists=details.log_file.exists,
            ),
        ),
    )


def _resolve_runtime_dir(config: ProjectConfig, runtime_path: str):
    return resolve_path(config.app_path.parent / runtime_path)
