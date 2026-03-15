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
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Disable one manual include row in SQLite.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--rules-config", default="config/rules.yaml")
    parser.add_argument("--manual-include-id", required=True, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    try:
        config = load_config(app_path=args.config, rules_path=args.rules_config)
        engine = create_engine_from_config(config)
        session_factory = create_session_factory(engine)

        with session_scope(session_factory) as session:
            repo = ManualIncludesRepository(session)
            manual_include = repo.get_by_id(args.manual_include_id)
            if manual_include is None:
                raise LookupError(
                    f"Manual include not found for id: {args.manual_include_id}"
                )
            if not manual_include.enabled:
                print(f"Manual include already disabled: {manual_include.id}")
                return 0

            repo.update(
                manual_include.id,
                relative_path=manual_include.relative_path,
                include_path_type=manual_include.include_path_type,
                recursive=manual_include.recursive,
                force_include=manual_include.force_include,
                enabled=False,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )

        print(f"Disabled manual include: {args.manual_include_id}")
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


if __name__ == "__main__":
    raise SystemExit(main())
