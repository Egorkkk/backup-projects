from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.adapters.filesystem.path_utils import resolve_path
from backup_projects.config import ConfigError, load_config
from backup_projects.services.run_visibility_service import (
    get_run_details,
    list_runs,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser("runs", description="List and inspect recorded runs.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    nested_subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = nested_subparsers.add_parser("list", description="List top-level runs.")
    list_parser.add_argument("--limit", type=int, default=100)

    show_parser = nested_subparsers.add_parser("show", description="Show one run in detail.")
    show_parser.add_argument("--run-id", type=int, required=True)

    export_parser = nested_subparsers.add_parser("export", description="Export one run HTML report.")
    export_parser.add_argument("--id", type=int, required=True)
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List and inspect recorded runs.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", description="List top-level runs.")
    list_parser.add_argument("--limit", type=int, default=100)

    show_parser = subparsers.add_parser("show", description="Show one run in detail.")
    show_parser.add_argument("--run-id", type=int, required=True)

    export_parser = subparsers.add_parser("export", description="Export one run HTML report.")
    export_parser.add_argument("--id", type=int, required=True)

    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)
        reports_dir = _resolve_runtime_dir(
            config.app_path.parent,
            config.app_config.runtime.reports_dir,
        )
        logs_dir = _resolve_runtime_dir(
            config.app_path.parent,
            config.app_config.runtime.logs_dir,
        )

        with session_scope(session_factory) as session:
            if args.command == "list":
                runs = list_runs(session=session, limit=args.limit)
                _print_runs_list(runs)
            elif args.command == "show":
                details = get_run_details(
                    session=session,
                    run_id=args.run_id,
                    reports_dir=reports_dir,
                    logs_dir=logs_dir,
                )
                _print_run_details(details)
            elif args.command == "export":
                details = get_run_details(
                    session=session,
                    run_id=args.id,
                    reports_dir=reports_dir,
                    logs_dir=logs_dir,
                )
                _write_run_export(details)
            else:
                raise ValueError(f"Unsupported runs command: {args.command}")
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except (LookupError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        engine = locals().get("engine")
        if engine is not None:
            engine.dispose()

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    return handle(args)


def _resolve_runtime_dir(base_dir: Path, configured_path: str) -> Path:
    return resolve_path(base_dir / configured_path)


def _print_runs_list(runs) -> None:
    print("Runs")
    if not runs:
        print("- none")
        return

    for run in runs:
        print(f"- id: {run.id}")
        print(f"  run-type: {run.run_type}")
        print(f"  status: {run.status}")
        print(f"  trigger-mode: {run.trigger_mode}")
        print(f"  started-at: {run.started_at}")
        print(f"  finished-at: {run.finished_at or '-'}")


def _print_run_details(details) -> None:
    print("Run")
    print(f"id: {details.run.id}")
    print(f"run-type: {details.run.run_type}")
    print(f"status: {details.run.status}")
    print(f"trigger-mode: {details.run.trigger_mode}")
    print(f"started-at: {details.run.started_at}")
    print(f"finished-at: {details.run.finished_at or '-'}")
    print("")
    print("Events")
    if not details.events:
        print("- none")
    else:
        for event in details.events:
            print(f"- {event.event_time} [{event.level}] {event.event_type}: {event.message}")
    print("")
    print("Artifacts")
    _print_artifact("report-json", details.report_json.path, details.report_json.exists)
    _print_artifact("report-text", details.report_text.path, details.report_text.exists)
    _print_artifact("report-html", details.report_html.path, details.report_html.exists)
    _print_artifact("log-file", details.log_file.path, details.log_file.exists)


def _print_artifact(label: str, path: str, exists: bool) -> None:
    print(f"{label}: {path}")
    print(f"{label}-exists: {'yes' if exists else 'no'}")


def _write_run_export(details) -> None:
    if not details.report_html.exists:
        raise ValueError(f"HTML report is missing for run id: {details.run.id}")
    try:
        sys.stdout.write(Path(details.report_html.path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"HTML report is missing for run id: {details.run.id}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
