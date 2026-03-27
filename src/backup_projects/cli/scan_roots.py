from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import ConfigError, load_config
from backup_projects.services.root_discovery_service import (
    RootDiscoveryResult,
    discover_and_sync_roots,
)


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "scan-roots",
        description="Discover configured RAID roots and sync them to SQLite.",
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.set_defaults(_handler=handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover configured RAID roots and sync them to SQLite."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    return parser


def handle(args: argparse.Namespace) -> int:
    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)
        discovered_at = datetime.now(timezone.utc).isoformat()
        enabled_roots = [root for root in config.app_config.raid_roots if root.enabled]

        if not enabled_roots:
            print("No enabled raid roots configured.")
            return 0

        for index, raid_root in enumerate(enabled_roots):
            with session_scope(session_factory) as session:
                result = discover_and_sync_roots(
                    session=session,
                    raid_name=raid_root.name,
                    raid_path=raid_root.path,
                    discovered_at=discovered_at,
                )
            _print_summary(raid_root.name, raid_root.path, result)
            if index < len(enabled_roots) - 1:
                print()
    except ConfigError as exc:
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


def _print_summary(raid_name: str, raid_path: str, result: RootDiscoveryResult) -> None:
    print(f"Scanned target: {raid_name} ({raid_path})")
    print(f"discovered: {len(result.discovered)}")
    print(f"created: {len(result.created)}")
    print(f"reactivated: {len(result.reactivated)}")
    print(f"marked-missing: {len(result.marked_missing)}")
    print(f"unchanged-present: {len(result.unchanged_present)}")


if __name__ == "__main__":
    raise SystemExit(main())
