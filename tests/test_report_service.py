import json
from pathlib import Path

from backup_projects.adapters.restic_adapter import ResticBackupResult
from backup_projects.domain.models import CandidateFile, FinalDecision, ManifestResult
from backup_projects.services.report_service import (
    RunReportTargetInput,
    build_run_report,
    write_run_report,
)
from backup_projects.services.run_service import RunLifecycleEvent, RunLifecycleRecord


def test_build_run_report_collects_run_events_and_targets() -> None:
    report = build_run_report(
        run=_make_run_record(),
        events=(_make_run_event(),),
        manifest_result=_make_manifest_result(),
        backup_result=_make_backup_result(),
        targets=(
            RunReportTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                included_count=1,
                skipped_count=0,
            ),
        ),
    )

    assert report.format_version == 1
    assert report.run.id == 42
    assert report.run.run_type == "daily"
    assert len(report.events) == 1
    assert report.events[0].event_type == "manifest_built"
    assert report.manifest is not None
    assert report.manifest.manifest_file_path == "/runtime/manifests/run.manifest.txt"
    assert report.backup is not None
    assert report.backup.snapshot_id == "snapshot-123"
    assert len(report.targets) == 1
    assert report.targets[0].root_id == 1
    assert report.targets[0].root_path == "/mnt/raid_a/show-a"
    assert report.targets[0].status == "completed"
    assert report.targets[0].included_count == 1
    assert report.targets[0].skipped_count == 0
    assert report.targets[0].manifest is None
    assert report.targets[0].backup is None


def test_write_run_report_writes_canonical_json_without_raw_process_fields(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    artifacts = write_run_report(
        reports_dir=reports_dir,
        run=_make_run_record(),
        events=(_make_run_event(),),
        manifest_result=_make_manifest_result(),
        backup_result=_make_backup_result(),
        targets=(
            RunReportTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                included_count=1,
                skipped_count=0,
            ),
        ),
    )

    assert Path(artifacts.report_dir) == reports_dir / "run-42"
    assert Path(artifacts.json_report_path) == reports_dir / "run-42" / "report.json"
    payload = json.loads(Path(artifacts.json_report_path).read_text(encoding="utf-8"))

    assert payload["format_version"] == 1
    assert payload["run"]["id"] == 42
    assert set(payload) == {
        "format_version",
        "run",
        "manifest",
        "backup",
        "events",
        "targets",
    }
    assert payload["manifest"] == {
        "manifest_file_path": "/runtime/manifests/run.manifest.txt",
        "json_manifest_file_path": "/runtime/manifests/run.manifest.json",
        "summary_file_path": "/runtime/manifests/run.summary.txt",
    }
    assert payload["backup"] == {
        "snapshot_id": "snapshot-123",
        "summary_payload": {
            "files_new": 2,
            "message_type": "summary",
            "snapshot_id": "snapshot-123",
        },
    }
    assert payload["events"][0] == {
        "event_time": "2026-03-17T10:01:00+00:00",
        "level": "INFO",
        "event_type": "manifest_built",
        "message": "Manifest built",
        "payload": {"included_count": 2},
    }
    assert "id" not in payload["events"][0]
    assert "run_id" not in payload["events"][0]
    assert payload["targets"][0]["included_count"] == 1
    assert payload["targets"][0]["skipped_count"] == 0
    assert payload["targets"][0]["manifest"] is None
    assert payload["targets"][0]["backup"] is None


def test_write_run_report_uses_deterministic_paths_for_all_renderers(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    artifacts = write_run_report(
        reports_dir=reports_dir,
        run=_make_run_record(),
        events=(_make_run_event(),),
        targets=(RunReportTargetInput(status="completed"),),
    )

    assert Path(artifacts.report_dir) == reports_dir / "run-42"
    assert Path(artifacts.json_report_path) == reports_dir / "run-42" / "report.json"
    assert Path(artifacts.text_report_path) == reports_dir / "run-42" / "report.txt"
    assert Path(artifacts.html_report_path) == reports_dir / "run-42" / "report.html"


def test_write_run_report_writes_human_readable_text_report(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    artifacts = write_run_report(
        reports_dir=reports_dir,
        run=_make_run_record(),
        events=(_make_run_event(),),
        targets=(
            RunReportTargetInput(
                root_id=1,
                root_path="/mnt/raid_a/show-a",
                status="completed",
                included_count=2,
                skipped_count=1,
            ),
        ),
        manifest_result=_make_manifest_result(),
        backup_result=_make_backup_result(),
    )

    text_report = Path(artifacts.text_report_path).read_text(encoding="utf-8")

    assert "Run report" in text_report
    assert "Run" in text_report
    assert "Manifest" in text_report
    assert "Backup" in text_report
    assert "Events" in text_report
    assert "Targets" in text_report
    assert "manifest_built" in text_report
    assert "snapshot-id: snapshot-123" in text_report
    assert "root-path: /mnt/raid_a/show-a" in text_report
    assert "included-count: 2" in text_report
    assert "skipped-count: 1" in text_report


def test_write_run_report_writes_static_html_export(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    artifacts = write_run_report(
        reports_dir=reports_dir,
        run=_make_run_record(),
        events=(_make_run_event(),),
        targets=(RunReportTargetInput(status="completed"),),
    )

    html_report = Path(artifacts.html_report_path).read_text(encoding="utf-8")

    assert "<!DOCTYPE html>" in html_report
    assert "<html" in html_report
    assert "Run report #42" in html_report
    assert "<h2>Manifest</h2>" in html_report
    assert "<h2>Backup</h2>" in html_report
    assert "<h2>Events</h2>" in html_report
    assert "<h2>Targets</h2>" in html_report


def test_write_run_report_handles_missing_optional_sections(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    artifacts = write_run_report(
        reports_dir=reports_dir,
        run=_make_run_record(),
        events=(),
        targets=(RunReportTargetInput(status="failed", error="backup failed"),),
    )

    payload = json.loads(Path(artifacts.json_report_path).read_text(encoding="utf-8"))

    assert payload["events"] == []
    assert payload["targets"] == [
        {
            "backup": None,
            "error": "backup failed",
            "included_count": 0,
            "manifest": None,
            "root_id": None,
            "root_path": None,
            "skipped_count": 0,
            "status": "failed",
        }
    ]
    assert Path(artifacts.html_report_path).is_file()
    assert Path(artifacts.text_report_path).is_file()


def _make_run_record() -> RunLifecycleRecord:
    return RunLifecycleRecord(
        id=42,
        run_type="daily",
        status="completed",
        started_at="2026-03-17T10:00:00+00:00",
        trigger_mode="cron",
        finished_at="2026-03-17T10:05:00+00:00",
    )


def _make_run_event() -> RunLifecycleEvent:
    return RunLifecycleEvent(
        id=1,
        run_id=42,
        event_time="2026-03-17T10:01:00+00:00",
        level="INFO",
        event_type="manifest_built",
        message="Manifest built",
        payload={"included_count": 2},
    )


def _make_manifest_result() -> ManifestResult:
    return ManifestResult(
        manifest_paths=("/mnt/raid_a/show-a/project.prproj",),
        decisions=(
            FinalDecision(
                candidate=CandidateFile(
                    absolute_path="/mnt/raid_a/show-a/project.prproj",
                    extension="prproj",
                    size_bytes=2048,
                    mtime_ns=100,
                    ctime_ns=90,
                ),
                include=True,
                reason="policy_include",
            ),
        ),
        manifest_file_path="/runtime/manifests/run.manifest.txt",
        json_manifest_file_path="/runtime/manifests/run.manifest.json",
        summary_file_path="/runtime/manifests/run.summary.txt",
    )


def _make_backup_result() -> ResticBackupResult:
    return ResticBackupResult(
        manifest_file_path="/runtime/manifests/run.manifest.txt",
        snapshot_id="snapshot-123",
        summary_payload={
            "message_type": "summary",
            "snapshot_id": "snapshot-123",
            "files_new": 2,
        },
        argv=("restic", "backup"),
        stdout="raw stdout should not be included",
        stderr="raw stderr should not be included",
        duration_seconds=1.25,
    )
