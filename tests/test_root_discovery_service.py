from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.domain.enums import RootStatus
from backup_projects.domain.models import RootRecord
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.services import root_discovery_service as root_discovery_module
from backup_projects.services.root_discovery_service import (
    discover_and_sync_roots,
    list_root_directories,
)


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "root-discovery.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_list_root_directories_returns_only_first_level_directories(tmp_path: Path) -> None:
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    (raid_path / "show-a").mkdir()
    (raid_path / "show-b").mkdir()
    (raid_path / "notes.txt").write_text("ignore me")
    (raid_path / "show-a" / "nested").mkdir()

    discovered = list_root_directories(raid_path)

    assert discovered == (
        root_discovery_module.DiscoveredRootCandidate(
            name="show-a",
            path=(raid_path / "show-a").resolve(strict=False).as_posix(),
        ),
        root_discovery_module.DiscoveredRootCandidate(
            name="show-b",
            path=(raid_path / "show-b").resolve(strict=False).as_posix(),
        ),
    )


def test_discover_and_sync_roots_creates_new_roots_and_marks_missing(
    db_session: Session,
    tmp_path: Path,
) -> None:
    repo = RootsRepository(db_session)
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    (raid_path / "show-a").mkdir()
    (raid_path / "show-b").mkdir()
    (raid_path / "ignored.txt").write_text("ignored")

    repo.create(
        raid_name="raid_a",
        name="old-show",
        path=(raid_path / "old-show").resolve(strict=False).as_posix(),
        device_id=1,
        inode=2,
        mtime_ns=3,
        ctime_ns=4,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
    )

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert [record.path for record in result.created] == [
        (raid_path / "show-a").resolve(strict=False).as_posix(),
        (raid_path / "show-b").resolve(strict=False).as_posix(),
    ]
    assert [record.needs_structural_rescan for record in result.created] == [True, True]
    assert [record.path for record in result.discovered] == [
        (raid_path / "show-a").resolve(strict=False).as_posix(),
        (raid_path / "show-b").resolve(strict=False).as_posix(),
    ]
    assert [record.path for record in result.marked_missing] == [
        (raid_path / "old-show").resolve(strict=False).as_posix()
    ]
    assert result.reactivated == ()
    assert result.unchanged_present == ()
    assert all(isinstance(record, RootRecord) for record in result.created)
    assert all(isinstance(record, RootRecord) for record in result.discovered)
    assert all(isinstance(record, RootRecord) for record in result.marked_missing)

    all_roots = repo.list_all()
    assert [record.path for record in all_roots] == [
        (raid_path / "old-show").resolve(strict=False).as_posix(),
        (raid_path / "show-a").resolve(strict=False).as_posix(),
        (raid_path / "show-b").resolve(strict=False).as_posix(),
    ]
    missing_root = repo.get_by_path((raid_path / "old-show").resolve(strict=False).as_posix())
    assert missing_root is not None
    assert missing_root.is_missing is True


def test_discover_and_sync_roots_reactivates_missing_and_preserves_needs_rescan(
    db_session: Session,
    tmp_path: Path,
) -> None:
    repo = RootsRepository(db_session)
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    (raid_path / "show-a").mkdir()
    (raid_path / "show-b").mkdir()

    show_a_path = (raid_path / "show-a").resolve(strict=False).as_posix()
    show_b_path = (raid_path / "show-b").resolve(strict=False).as_posix()
    existing_present = repo.create(
        raid_name="raid_a",
        name="show-a",
        path=show_a_path,
        device_id=None,
        inode=None,
        mtime_ns=None,
        ctime_ns=None,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
        needs_structural_rescan=True,
    )
    existing_missing = repo.create(
        raid_name="raid_a",
        name="show-b",
        path=show_b_path,
        device_id=None,
        inode=None,
        mtime_ns=None,
        ctime_ns=None,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
        is_missing=True,
        needs_structural_rescan=True,
    )

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert [record.path for record in result.reactivated] == [show_b_path]
    assert [record.path for record in result.unchanged_present] == [show_a_path]
    assert [record.status for record in result.discovered] == [
        RootStatus.ACTIVE,
        RootStatus.ACTIVE,
    ]
    refreshed_present = repo.get_by_id(existing_present.id)
    refreshed_missing = repo.get_by_id(existing_missing.id)
    assert refreshed_present is not None
    assert refreshed_missing is not None
    assert refreshed_present.needs_structural_rescan is True
    assert refreshed_missing.needs_structural_rescan is True
    assert refreshed_present.last_seen_at == "2026-03-13T10:00:00+00:00"
    assert refreshed_missing.is_missing is False
    assert refreshed_missing.last_seen_at == "2026-03-13T10:00:00+00:00"


def test_discover_and_sync_roots_sets_rescan_true_when_existing_root_stat_changes(
    db_session: Session,
    tmp_path: Path,
) -> None:
    repo = RootsRepository(db_session)
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    (raid_path / "show-a").mkdir()

    show_a_path = (raid_path / "show-a").resolve(strict=False).as_posix()
    existing_root = repo.create(
        raid_name="raid_a",
        name="show-a",
        path=show_a_path,
        device_id=1,
        inode=2,
        mtime_ns=3,
        ctime_ns=4,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
        needs_structural_rescan=False,
    )

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert [record.path for record in result.unchanged_present] == [show_a_path]
    assert [record.needs_structural_rescan for record in result.discovered] == [True]
    refreshed_root = repo.get_by_id(existing_root.id)
    assert refreshed_root is not None
    assert refreshed_root.needs_structural_rescan is True


def test_discover_and_sync_roots_preserves_false_rescan_flag_when_stat_is_unchanged(
    db_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = RootsRepository(db_session)
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    show_a_dir = raid_path / "show-a"
    show_a_dir.mkdir()

    original_read_stat = root_discovery_module.read_stat
    initial_stat = original_read_stat(show_a_dir)
    assert initial_stat is not None

    existing_root = repo.create(
        raid_name="raid_a",
        name="show-a",
        path=show_a_dir.resolve(strict=False).as_posix(),
        device_id=999999,
        inode=initial_stat.inode,
        mtime_ns=initial_stat.mtime_ns,
        ctime_ns=initial_stat.ctime_ns,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
        needs_structural_rescan=False,
    )

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert [record.needs_structural_rescan for record in result.unchanged_present] == [False]
    refreshed_root = repo.get_by_id(existing_root.id)
    assert refreshed_root is not None
    assert refreshed_root.needs_structural_rescan is False


def test_discover_and_sync_roots_skips_disappeared_child_after_listing_race(
    db_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raid_path = tmp_path / "raid_a"
    raid_path.mkdir()
    disappearing_dir = raid_path / "show-a"
    disappearing_dir.mkdir()

    original_read_stat = root_discovery_module.read_stat

    def fake_read_stat(path, *, follow_symlinks=False):
        if Path(path) == disappearing_dir:
            return None
        return original_read_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(root_discovery_module, "read_stat", fake_read_stat)

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    assert result.discovered == ()
    assert result.created == ()
    assert result.marked_missing == ()
    assert RootsRepository(db_session).list_all() == []


def test_discover_and_sync_roots_keeps_other_raid_roots_active_across_multiple_passes(
    db_session: Session,
    tmp_path: Path,
) -> None:
    repo = RootsRepository(db_session)
    raid_a_path = tmp_path / "raid_a"
    raid_b_path = tmp_path / "raid_b"
    raid_a_path.mkdir()
    raid_b_path.mkdir()
    (raid_a_path / "show-a").mkdir()
    (raid_b_path / "show-b").mkdir()

    first_result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_a_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )
    second_result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_b",
        raid_path=raid_b_path,
        discovered_at="2026-03-13T10:05:00+00:00",
    )

    assert [record.path for record in first_result.created] == [
        (raid_a_path / "show-a").resolve(strict=False).as_posix()
    ]
    assert [record.path for record in second_result.created] == [
        (raid_b_path / "show-b").resolve(strict=False).as_posix()
    ]
    assert second_result.marked_missing == ()
    assert [record.path for record in repo.list_active()] == [
        (raid_a_path / "show-a").resolve(strict=False).as_posix(),
        (raid_b_path / "show-b").resolve(strict=False).as_posix(),
    ]


def test_discover_and_sync_roots_marks_missing_only_within_current_raid_scope(
    db_session: Session,
    tmp_path: Path,
) -> None:
    repo = RootsRepository(db_session)
    raid_a_path = tmp_path / "raid_a"
    raid_b_path = tmp_path / "raid_b"
    raid_a_path.mkdir()
    raid_b_path.mkdir()
    (raid_a_path / "show-a").mkdir()
    (raid_b_path / "show-b").mkdir()

    raid_a_missing_path = (raid_a_path / "missing-a").resolve(strict=False).as_posix()
    raid_b_show_path = (raid_b_path / "show-b").resolve(strict=False).as_posix()

    missing_a = repo.create(
        raid_name="raid_a",
        name="missing-a",
        path=raid_a_missing_path,
        device_id=1,
        inode=2,
        mtime_ns=3,
        ctime_ns=4,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
    )
    present_b = repo.create(
        raid_name="raid_b",
        name="show-b",
        path=raid_b_show_path,
        device_id=11,
        inode=12,
        mtime_ns=13,
        ctime_ns=14,
        first_seen_at="2026-03-13T09:00:00+00:00",
        last_seen_at="2026-03-13T09:00:00+00:00",
    )

    result = discover_and_sync_roots(
        session=db_session,
        raid_name="raid_a",
        raid_path=raid_a_path,
        discovered_at="2026-03-13T10:00:00+00:00",
    )

    refreshed_missing_a = repo.get_by_id(missing_a.id)
    refreshed_present_b = repo.get_by_id(present_b.id)

    assert [record.path for record in result.marked_missing] == [raid_a_missing_path]
    assert refreshed_missing_a is not None
    assert refreshed_missing_a.is_missing is True
    assert refreshed_present_b is not None
    assert refreshed_present_b.is_missing is False
    assert [record.path for record in repo.list_active()] == [
        (raid_a_path / "show-a").resolve(strict=False).as_posix(),
        raid_b_show_path,
    ]
