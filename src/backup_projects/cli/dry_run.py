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
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.dry_run_service import build_root_dry_run_manifest
from backup_projects.services.manifest_builder import write_manifest


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "dry-run",
        description=(
            "Simulate policy selection from the current inventory without running "
            "backup."
        ),
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    root_group = parser.add_mutually_exclusive_group(required=True)
    root_group.add_argument("--root-id", type=int)
    root_group.add_argument("--root-path")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--artifact-stem")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate policy selection from the current inventory without running "
            "backup."
        )
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    root_group = parser.add_mutually_exclusive_group(required=True)
    root_group.add_argument("--root-id", type=int)
    root_group.add_argument("--root-path")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--artifact-stem")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        _validate_persistence_args(
            output_dir=args.output_dir,
            artifact_stem=args.artifact_stem,
        )
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            root = _resolve_target_root(
                session=session,
                root_id=args.root_id,
                root_path=args.root_path,
            )
            built_manifest = build_root_dry_run_manifest(
                session=session,
                root_id=root.id,
            )
            manifest_result = None
            if args.output_dir is not None and args.artifact_stem is not None:
                try:
                    manifest_result = write_manifest(
                        built_manifest=built_manifest,
                        output_dir=args.output_dir,
                        artifact_stem=args.artifact_stem,
                    )
                except ValueError as exc:
                    raise RuntimeError(str(exc)) from exc

        _print_dry_run_summary(
            root_id=root.id,
            root_path=root.path,
            summary_text=built_manifest.summary_text,
        )
        if manifest_result is not None:
            _print_artifact_paths(manifest_result)
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


def _resolve_target_root(*, session, root_id: int | None, root_path: str | None):
    repo = RootsRepository(session)
    if root_id is not None:
        root = repo.get_by_id(root_id)
        if root is None:
            raise LookupError(f"Root not found for id: {root_id}")
        return root

    if root_path is None:
        raise ValueError("Either --root-id or --root-path must be provided")

    resolved_root_path = resolve_path(root_path).as_posix()
    root = repo.get_by_path(resolved_root_path)
    if root is None:
        raise LookupError(f"Root not found for path: {resolved_root_path}")
    return root


def _validate_persistence_args(
    *,
    output_dir: Path | None,
    artifact_stem: str | None,
) -> None:
    if (output_dir is None) != (artifact_stem is None):
        raise ValueError(
            "--output-dir and --artifact-stem must be provided together"
        )
    if artifact_stem is not None and artifact_stem.strip() == "":
        raise ValueError("artifact_stem must not be empty")


def _print_dry_run_summary(*, root_id: int, root_path: str, summary_text: str) -> None:
    print(f"Dry run for root-id: {root_id}")
    print(f"root-path: {root_path}")
    print()
    print(summary_text)


def _print_artifact_paths(manifest_result) -> None:
    print()
    print(f"manifest-file: {manifest_result.manifest_file_path}")
    print(f"json-manifest-file: {manifest_result.json_manifest_file_path}")
    print(f"summary-file: {manifest_result.summary_file_path}")


if __name__ == "__main__":
    raise SystemExit(main())
