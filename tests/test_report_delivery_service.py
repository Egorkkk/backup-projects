from pathlib import Path

from backup_projects.services.report_delivery_service import (
    ReportDeliveryRequest,
    run_report_delivery,
)


def test_run_report_delivery_success_copies_canonical_text_report(tmp_path: Path) -> None:
    source_report_path = tmp_path / "runtime" / "reports" / "run-7" / "report.txt"
    source_report_path.parent.mkdir(parents=True, exist_ok=True)
    source_report_path.write_text("run report text\n", encoding="utf-8")

    result = run_report_delivery(
        ReportDeliveryRequest(
            run_id=7,
            mode="local_file",
            source_report_path=str(source_report_path),
            output_dir=str(tmp_path / "delivered"),
        )
    )

    assert result.status == "completed"
    assert result.destination_path is not None
    delivered_path = Path(result.destination_path)
    assert delivered_path.name == "run-7-report.txt"
    assert delivered_path.read_text(encoding="utf-8") == "run report text\n"


def test_run_report_delivery_returns_clear_failure_when_source_report_is_missing(
    tmp_path: Path,
) -> None:
    missing_source_path = tmp_path / "runtime" / "reports" / "run-8" / "report.txt"

    result = run_report_delivery(
        ReportDeliveryRequest(
            run_id=8,
            mode="local_file",
            source_report_path=str(missing_source_path),
            output_dir=str(tmp_path / "delivered"),
        )
    )

    assert result.status == "failed"
    assert result.destination_path is None
    assert result.error == f"canonical text report is missing: {missing_source_path}"


def test_run_report_delivery_returns_clear_failure_on_destination_filesystem_error(
    tmp_path: Path,
) -> None:
    source_report_path = tmp_path / "runtime" / "reports" / "run-9" / "report.txt"
    source_report_path.parent.mkdir(parents=True, exist_ok=True)
    source_report_path.write_text("run report text\n", encoding="utf-8")

    output_dir_as_file = tmp_path / "delivered"
    output_dir_as_file.write_text("not a directory", encoding="utf-8")

    result = run_report_delivery(
        ReportDeliveryRequest(
            run_id=9,
            mode="local_file",
            source_report_path=str(source_report_path),
            output_dir=str(output_dir_as_file),
        )
    )

    assert result.status == "failed"
    assert result.destination_path == str(output_dir_as_file / "run-9-report.txt")
    assert result.error is not None
