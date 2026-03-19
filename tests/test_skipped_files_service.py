from pathlib import Path

import pytest

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.domain.models import CandidateFile, FinalDecision
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services.manifest_builder import BuiltManifest
from backup_projects.services.skipped_files_service import list_skipped_files


@pytest.fixture
def session_factory(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "skipped-files.sqlite3")
    create_schema(engine)
    factory = create_session_factory(engine)
    try:
        yield factory
    finally:
        engine.dispose()


def test_list_skipped_files_uses_dry_run_flow_and_returns_current_skipped_paths(
    monkeypatch,
    session_factory,
) -> None:
    captured_calls: list[tuple] = []

    with session_scope(session_factory) as session:
        root = RootsRepository(session).create(
            raid_name="raid_a",
            name="show-a",
            path="/mnt/raid_a/show-a",
            device_id=1,
            inode=2,
            mtime_ns=3,
            ctime_ns=4,
            first_seen_at="2026-03-17T10:00:00+00:00",
            last_seen_at="2026-03-17T10:00:00+00:00",
        )

        def fake_build_root_dry_run_manifest(*, session, root_id):
            captured_calls.append(("build_root_dry_run_manifest", session, root_id))
            return BuiltManifest(
                manifest_paths=("/mnt/raid_a/show-a/included.prproj",),
                decisions=(
                    FinalDecision(
                        candidate=CandidateFile(
                            absolute_path="/mnt/raid_a/show-a/included.prproj",
                            extension="prproj",
                            size_bytes=10,
                            mtime_ns=1,
                            ctime_ns=1,
                        ),
                        include=True,
                        reason="policy_include",
                    ),
                    FinalDecision(
                        candidate=CandidateFile(
                            absolute_path="/mnt/raid_a/show-a/skipped.tmp",
                            extension="tmp",
                            size_bytes=5,
                            mtime_ns=1,
                            ctime_ns=1,
                        ),
                        include=False,
                        reason="excluded",
                    ),
                ),
                json_payload={},
                summary_text="Manifest summary",
            )

        monkeypatch.setattr(
            "backup_projects.services.skipped_files_service.build_root_dry_run_manifest",
            fake_build_root_dry_run_manifest,
        )

        result = list_skipped_files(session=session, root_id=root.id)

    assert captured_calls == [("build_root_dry_run_manifest", session, root.id)]
    assert result.root_id == root.id
    assert result.root_path == "/mnt/raid_a/show-a"
    assert len(result.skipped_files) == 1
    assert result.skipped_files[0].path == "/mnt/raid_a/show-a/skipped.tmp"
    assert result.skipped_files[0].reason == "excluded"


def test_list_skipped_files_resolves_root_path_and_raises_for_unknown_root(
    monkeypatch,
    session_factory,
) -> None:
    with session_scope(session_factory) as session:
        RootsRepository(session).create(
            raid_name="raid_a",
            name="show-a",
            path="/resolved/show-a",
            device_id=1,
            inode=2,
            mtime_ns=3,
            ctime_ns=4,
            first_seen_at="2026-03-17T10:00:00+00:00",
            last_seen_at="2026-03-17T10:00:00+00:00",
        )

        monkeypatch.setattr(
            "backup_projects.services.skipped_files_service.resolve_path",
            lambda path: Path("/resolved/show-a"),
        )
        monkeypatch.setattr(
            "backup_projects.services.skipped_files_service.build_root_dry_run_manifest",
            lambda *, session, root_id: BuiltManifest(
                manifest_paths=(),
                decisions=(),
                json_payload={},
                summary_text="Manifest summary",
            ),
        )

        result = list_skipped_files(session=session, root_path="~/show-a")

        assert result.root_path == "/resolved/show-a"

        with pytest.raises(LookupError, match="^Root not found for id: 999$"):
            list_skipped_files(session=session, root_id=999)
