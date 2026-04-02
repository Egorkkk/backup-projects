from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ReportDeliveryRequest:
    run_id: int
    mode: str
    source_report_path: str
    output_dir: str


@dataclass(frozen=True, slots=True)
class ReportDeliveryResult:
    status: str
    mode: str
    source_report_path: str
    destination_path: str | None = None
    error: str | None = None


def run_report_delivery(request: ReportDeliveryRequest) -> ReportDeliveryResult:
    if request.mode != "local_file":
        return ReportDeliveryResult(
            status="failed",
            mode=request.mode,
            source_report_path=request.source_report_path,
            error=f"unsupported report delivery mode: {request.mode}",
        )

    source_path = Path(request.source_report_path)
    if not source_path.exists() or not source_path.is_file():
        return ReportDeliveryResult(
            status="failed",
            mode=request.mode,
            source_report_path=request.source_report_path,
            error=f"canonical text report is missing: {source_path}",
        )

    output_dir = Path(request.output_dir)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        destination_path = output_dir / _build_delivered_report_filename(run_id=request.run_id)
        shutil.copy2(source_path, destination_path)
    except OSError as exc:
        return ReportDeliveryResult(
            status="failed",
            mode=request.mode,
            source_report_path=request.source_report_path,
            destination_path=str(output_dir / _build_delivered_report_filename(run_id=request.run_id)),
            error=str(exc),
        )

    return ReportDeliveryResult(
        status="completed",
        mode=request.mode,
        source_report_path=request.source_report_path,
        destination_path=str(destination_path),
    )


def _build_delivered_report_filename(*, run_id: int) -> str:
    return f"run-{run_id}-report.txt"
