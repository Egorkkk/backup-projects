from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from html import escape
from pathlib import Path

from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain.models import ManifestResult
from backup_projects.services.run_service import RunLifecycleEvent, RunLifecycleRecord

REPORT_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class RunReportManifest:
    manifest_file_path: str
    json_manifest_file_path: str
    summary_file_path: str


@dataclass(frozen=True, slots=True)
class RunReportBackup:
    snapshot_id: str
    summary_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class RunReportTarget:
    status: str
    root_id: int | None = None
    root_path: str | None = None
    included_count: int = 0
    skipped_count: int = 0
    manifest: RunReportManifest | None = None
    backup: RunReportBackup | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RunReport:
    format_version: int
    run: RunLifecycleRecord
    events: tuple[RunLifecycleEvent, ...]
    targets: tuple[RunReportTarget, ...]
    manifest: RunReportManifest | None = None
    backup: RunReportBackup | None = None


@dataclass(frozen=True, slots=True)
class RunReportTargetInput:
    status: str
    root_id: int | None = None
    root_path: str | None = None
    included_count: int | None = None
    skipped_count: int | None = None
    manifest_result: ManifestResult | None = None
    backup_result: ResticBackupResult | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RunReportArtifacts:
    report: RunReport
    report_dir: str
    json_report_path: str
    text_report_path: str
    html_report_path: str


def build_run_report(
    *,
    run: RunLifecycleRecord,
    events: Iterable[RunLifecycleEvent],
    targets: Iterable[RunReportTargetInput],
    manifest_result: ManifestResult | None = None,
    backup_result: ResticBackupResult | None = None,
) -> RunReport:
    return RunReport(
        format_version=REPORT_FORMAT_VERSION,
        run=run,
        events=tuple(events),
        manifest=_to_run_report_manifest(manifest_result),
        backup=_to_run_report_backup(backup_result),
        targets=tuple(_to_run_report_target(target) for target in targets),
    )


def write_run_report(
    *,
    reports_dir: Path,
    run: RunLifecycleRecord,
    events: Iterable[RunLifecycleEvent],
    targets: Iterable[RunReportTargetInput],
    manifest_result: ManifestResult | None = None,
    backup_result: ResticBackupResult | None = None,
) -> RunReportArtifacts:
    normalized_reports_dir = _validate_reports_dir(reports_dir)
    report = build_run_report(
        run=run,
        events=events,
        targets=targets,
        manifest_result=manifest_result,
        backup_result=backup_result,
    )
    report_dir = normalized_reports_dir / f"run-{report.run.id}"
    report_dir.mkdir(parents=True, exist_ok=True)

    json_report_path = report_dir / "report.json"
    text_report_path = report_dir / "report.txt"
    html_report_path = report_dir / "report.html"

    json_report_path.write_text(_render_report_json(report) + "\n", encoding="utf-8")
    text_report_path.write_text(_render_report_text(report) + "\n", encoding="utf-8")
    html_report_path.write_text(_render_report_html(report) + "\n", encoding="utf-8")

    return RunReportArtifacts(
        report=report,
        report_dir=str(report_dir),
        json_report_path=str(json_report_path),
        text_report_path=str(text_report_path),
        html_report_path=str(html_report_path),
    )


def _to_run_report_target(target: RunReportTargetInput) -> RunReportTarget:
    included_count, skipped_count = _extract_target_manifest_counts(target)
    return RunReportTarget(
        status=target.status,
        root_id=target.root_id,
        root_path=target.root_path,
        included_count=included_count,
        skipped_count=skipped_count,
        manifest=_to_run_report_manifest(target.manifest_result),
        backup=_to_run_report_backup(target.backup_result),
        error=target.error,
    )


def _extract_target_manifest_counts(target: RunReportTargetInput) -> tuple[int, int]:
    if target.included_count is not None or target.skipped_count is not None:
        return (target.included_count or 0, target.skipped_count or 0)

    if target.manifest_result is None:
        return (0, 0)

    included_count = sum(1 for decision in target.manifest_result.decisions if decision.include)
    skipped_count = sum(1 for decision in target.manifest_result.decisions if not decision.include)
    return (included_count, skipped_count)


def _to_run_report_manifest(manifest_result: ManifestResult | None) -> RunReportManifest | None:
    if manifest_result is None:
        return None
    return RunReportManifest(
        manifest_file_path=manifest_result.manifest_file_path,
        json_manifest_file_path=manifest_result.json_manifest_file_path,
        summary_file_path=manifest_result.summary_file_path,
    )


def _to_run_report_backup(backup_result: ResticBackupResult | None) -> RunReportBackup | None:
    if backup_result is None:
        return None
    return RunReportBackup(
        snapshot_id=backup_result.snapshot_id,
        summary_payload=dict(backup_result.summary_payload),
    )


def _validate_reports_dir(reports_dir: Path) -> Path:
    if not reports_dir.exists():
        raise ValueError(f"reports_dir does not exist: {reports_dir}")
    if not reports_dir.is_dir():
        raise ValueError(f"reports_dir is not a directory: {reports_dir}")
    return reports_dir


def _render_report_json(report: RunReport) -> str:
    return json.dumps(_to_report_payload(report), indent=2, sort_keys=True)


def _render_report_text(report: RunReport) -> str:
    lines = [
        "Run report",
        f"Format version: {report.format_version}",
        "",
        "Run",
        f"id: {report.run.id}",
        f"run-type: {report.run.run_type}",
        f"trigger-mode: {report.run.trigger_mode}",
        f"status: {report.run.status}",
        f"started-at: {report.run.started_at}",
        f"finished-at: {report.run.finished_at or '-'}",
    ]

    lines.extend(["", "Manifest"])
    if report.manifest is None:
        lines.append("- none")
    else:
        lines.append(f"manifest-file: {report.manifest.manifest_file_path}")
        lines.append(f"json-manifest-file: {report.manifest.json_manifest_file_path}")
        lines.append(f"summary-file: {report.manifest.summary_file_path}")

    lines.extend(["", "Backup"])
    if report.backup is None:
        lines.append("- none")
    else:
        lines.append(f"snapshot-id: {report.backup.snapshot_id}")
        lines.append(
            "summary-payload: "
            f"{json.dumps(report.backup.summary_payload, sort_keys=True)}"
        )

    lines.extend(["", "Events"])

    if not report.events:
        lines.append("- none")
    else:
        for event in report.events:
            lines.append(
                f"- {event.event_time} [{event.level}] {event.event_type}: {event.message}"
            )
            if event.payload is not None:
                lines.append(f"  payload: {json.dumps(event.payload, sort_keys=True)}")

    lines.extend(["", "Targets"])
    if not report.targets:
        lines.append("- none")
    else:
        for index, target in enumerate(report.targets, start=1):
            lines.append(f"- target {index}")
            lines.append(f"  status: {target.status}")
            lines.append(f"  root-id: {target.root_id if target.root_id is not None else '-'}")
            lines.append(f"  root-path: {target.root_path or '-'}")
            lines.append(f"  included-count: {target.included_count}")
            lines.append(f"  skipped-count: {target.skipped_count}")
            lines.append(f"  error: {target.error or '-'}")
            if target.manifest is None:
                lines.append("  manifest: -")
            else:
                lines.append(f"  manifest-file: {target.manifest.manifest_file_path}")
                lines.append(
                    "  json-manifest-file: "
                    f"{target.manifest.json_manifest_file_path}"
                )
                lines.append(f"  summary-file: {target.manifest.summary_file_path}")
            if target.backup is None:
                lines.append("  backup: -")
            else:
                lines.append(f"  snapshot-id: {target.backup.snapshot_id}")
                lines.append(
                    "  summary-payload: "
                    f"{json.dumps(target.backup.summary_payload, sort_keys=True)}"
                )

    return "\n".join(lines)


def _render_report_html(report: RunReport) -> str:
    payload = _to_report_payload(report)
    events_html = "".join(
        [
            "<li>"
            f"<strong>{escape(event['event_time'])}</strong> "
            f"[{escape(event['level'])}] "
            f"{escape(event['event_type'])}: {escape(event['message'])}"
            f"{_render_html_json_block(event['payload'])}"
            "</li>"
            for event in payload["events"]
        ]
    )
    targets_html = "".join(
        [_render_target_html(target) for target in payload["targets"]]
    )
    return (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\">\n"
        f"  <title>Run report #{report.run.id}</title>\n"
        "  <style>"
        "body{font-family:sans-serif;margin:2rem;line-height:1.5;}"
        "section{margin-bottom:2rem;}"
        "pre{background:#f5f5f5;padding:1rem;overflow:auto;}"
        "li{margin-bottom:0.75rem;}"
        "h1,h2,h3{margin-bottom:0.5rem;}"
        "ul{padding-left:1.25rem;}"
        "dl{display:grid;grid-template-columns:max-content 1fr;gap:0.35rem 1rem;}"
        "dt{font-weight:bold;}"
        "dd{margin:0;}"
        "article{border:1px solid #ddd;padding:1rem;margin-bottom:1rem;}"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"  <h1>Run report #{report.run.id}</h1>\n"
        "  <section>\n"
        "    <h2>Run</h2>\n"
        "    <dl>\n"
        f"      <dt>Format version</dt><dd>{report.format_version}</dd>\n"
        f"      <dt>Run type</dt><dd>{escape(report.run.run_type)}</dd>\n"
        f"      <dt>Trigger mode</dt><dd>{escape(report.run.trigger_mode)}</dd>\n"
        f"      <dt>Status</dt><dd>{escape(report.run.status)}</dd>\n"
        f"      <dt>Started at</dt><dd>{escape(report.run.started_at)}</dd>\n"
        f"      <dt>Finished at</dt><dd>{escape(report.run.finished_at or '-')}</dd>\n"
        "    </dl>\n"
        "  </section>\n"
        "  <section>\n"
        "    <h2>Manifest</h2>\n"
        f"    {_render_run_manifest_html(payload['manifest'])}\n"
        "  </section>\n"
        "  <section>\n"
        "    <h2>Backup</h2>\n"
        f"    {_render_run_backup_html(payload['backup'])}\n"
        "  </section>\n"
        "  <section>\n"
        "    <h2>Events</h2>\n"
        f"    <ul>{events_html or '<li>none</li>'}</ul>\n"
        "  </section>\n"
        "  <section>\n"
        "    <h2>Targets</h2>\n"
        f"    {targets_html or '<p>none</p>'}\n"
        "  </section>\n"
        "</body>\n"
        "</html>"
    )


def _render_target_html(target: dict[str, object]) -> str:
    manifest = target["manifest"]
    backup = target["backup"]
    manifest_html = (
        "<p>Manifest: none</p>"
        if manifest is None
        else (
            "<dl>"
            f"<dt>Manifest file</dt><dd>{escape(str(manifest['manifest_file_path']))}</dd>"
            "<dt>JSON manifest file</dt>"
            f"<dd>{escape(str(manifest['json_manifest_file_path']))}</dd>"
            f"<dt>Summary file</dt><dd>{escape(str(manifest['summary_file_path']))}</dd>"
            "</dl>"
        )
    )
    backup_html = (
        "<p>Backup: none</p>"
        if backup is None
        else (
            "<dl>"
            f"<dt>Snapshot id</dt><dd>{escape(str(backup['snapshot_id']))}</dd>"
            f"<dt>Summary payload</dt><dd>{_render_html_json_block(backup['summary_payload'])}</dd>"
            "</dl>"
        )
    )
    return (
        "<article>"
        f"<h3>{escape(str(target['root_path'] or 'target'))}</h3>"
        "<dl>"
        f"<dt>Status</dt><dd>{escape(str(target['status']))}</dd>"
        f"<dt>Root id</dt><dd>{escape(str(target['root_id']))}</dd>"
        f"<dt>Root path</dt><dd>{escape(str(target['root_path']))}</dd>"
        f"<dt>Included count</dt><dd>{escape(str(target['included_count']))}</dd>"
        f"<dt>Skipped count</dt><dd>{escape(str(target['skipped_count']))}</dd>"
        f"<dt>Error</dt><dd>{escape(str(target['error']))}</dd>"
        "</dl>"
        f"{manifest_html}"
        f"{backup_html}"
        "</article>"
    )


def _render_run_manifest_html(manifest: object) -> str:
    if manifest is None:
        return "<p>none</p>"
    return (
        "<dl>"
        f"<dt>Manifest file</dt><dd>{escape(str(manifest['manifest_file_path']))}</dd>"
        "<dt>JSON manifest file</dt>"
        f"<dd>{escape(str(manifest['json_manifest_file_path']))}</dd>"
        f"<dt>Summary file</dt><dd>{escape(str(manifest['summary_file_path']))}</dd>"
        "</dl>"
    )


def _render_run_backup_html(backup: object) -> str:
    if backup is None:
        return "<p>none</p>"
    return (
        "<dl>"
        f"<dt>Snapshot id</dt><dd>{escape(str(backup['snapshot_id']))}</dd>"
        f"<dt>Summary payload</dt><dd>{_render_html_json_block(backup['summary_payload'])}</dd>"
        "</dl>"
    )


def _render_html_json_block(payload: object) -> str:
    if payload is None:
        return ""
    return f"<pre>{escape(json.dumps(payload, indent=2, sort_keys=True))}</pre>"


def _to_report_payload(report: RunReport) -> dict[str, object]:
    return {
        "format_version": report.format_version,
        "run": asdict(report.run),
        "manifest": asdict(report.manifest) if report.manifest is not None else None,
        "backup": asdict(report.backup) if report.backup is not None else None,
        "events": [_event_to_payload(event) for event in report.events],
        "targets": [_target_to_payload(target) for target in report.targets],
    }


def _event_to_payload(event: RunLifecycleEvent) -> dict[str, object]:
    return {
        "event_time": event.event_time,
        "level": event.level,
        "event_type": event.event_type,
        "message": event.message,
        "payload": event.payload,
    }


def _target_to_payload(target: RunReportTarget) -> dict[str, object]:
    return {
        "root_id": target.root_id,
        "root_path": target.root_path,
        "status": target.status,
        "included_count": target.included_count,
        "skipped_count": target.skipped_count,
        "manifest": asdict(target.manifest) if target.manifest is not None else None,
        "backup": asdict(target.backup) if target.backup is not None else None,
        "error": target.error,
    }
