from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.repositories.rules_repo import RulesRepository
from backup_projects.repositories.runs_repo import RunsRepository


@pytest.fixture
def db_session(tmp_path: Path):
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "repos.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        yield session

    engine.dispose()


def test_roots_repository_create_lookup_list_and_update(db_session: Session) -> None:
    repo = RootsRepository(db_session)

    created = repo.create(
        raid_name="raid_a",
        name="project-root",
        path="/mnt/raid_a/projects/project-root",
        device_id=100,
        inode=200,
        mtime_ns=300,
        ctime_ns=400,
        first_seen_at="2026-03-13T10:00:00+00:00",
        last_seen_at="2026-03-13T10:00:00+00:00",
    )

    by_id = repo.get_by_id(created.id)
    by_path = repo.get_by_path(created.path)
    active = repo.list_active()

    assert by_id == created
    assert by_path == created
    assert active == [created]

    repo.mark_missing(created.id, last_seen_at="2026-03-13T11:00:00+00:00")
    assert repo.list_active() == []

    repo.mark_present(
        created.id,
        device_id=101,
        inode=201,
        mtime_ns=301,
        ctime_ns=401,
        last_seen_at="2026-03-13T12:00:00+00:00",
        needs_structural_rescan=True,
    )
    refreshed = repo.get_by_id(created.id)

    assert refreshed is not None
    assert refreshed.is_missing is False
    assert refreshed.needs_structural_rescan is True
    assert refreshed.device_id == 101


def test_project_dirs_and_project_files_repositories_baseline_flow(db_session: Session) -> None:
    roots_repo = RootsRepository(db_session)
    dirs_repo = ProjectDirsRepository(db_session)
    files_repo = ProjectFilesRepository(db_session)

    root = roots_repo.create(
        raid_name="raid_a",
        name="root-1",
        path="/mnt/raid_a/projects/root-1",
        device_id=100,
        inode=200,
        mtime_ns=300,
        ctime_ns=400,
        first_seen_at="2026-03-13T10:00:00+00:00",
        last_seen_at="2026-03-13T10:00:00+00:00",
    )
    project_dir = dirs_repo.create(
        root_id=root.id,
        relative_path="show-a/episode-1",
        name="episode-1",
        dir_type="premiere",
        first_seen_at="2026-03-13T10:01:00+00:00",
        last_seen_at="2026-03-13T10:01:00+00:00",
    )
    project_file = files_repo.create(
        project_dir_id=project_dir.id,
        relative_path="show-a/episode-1/edit.prproj",
        filename="edit.prproj",
        extension="prproj",
        size_bytes=1024,
        mtime_ns=1000,
        ctime_ns=900,
        inode=555,
        first_seen_at="2026-03-13T10:02:00+00:00",
        last_seen_at="2026-03-13T10:02:00+00:00",
    )

    assert dirs_repo.get_by_root_and_path(
        root_id=root.id, relative_path="show-a/episode-1"
    ) == project_dir
    assert dirs_repo.list_by_root(root.id) == [project_dir]
    assert files_repo.get_by_dir_and_path(
        project_dir_id=project_dir.id,
        relative_path="show-a/episode-1/edit.prproj",
    ) == project_file

    dirs_repo.update_scan_state(
        project_dir.id,
        dir_type="mixed",
        is_missing=False,
        last_seen_at="2026-03-13T11:00:00+00:00",
    )
    files_repo.update_stat_fields(
        project_file.id,
        size_bytes=2048,
        mtime_ns=1100,
        ctime_ns=950,
        inode=556,
        is_missing=False,
        last_seen_at="2026-03-13T11:01:00+00:00",
    )

    refreshed_dir = dirs_repo.get_by_id(project_dir.id)
    refreshed_file = files_repo.get_by_id(project_file.id)

    assert refreshed_dir is not None
    assert refreshed_dir.dir_type == "mixed"
    assert refreshed_file is not None
    assert refreshed_file.size_bytes == 2048
    assert refreshed_file.inode == 556

    files_repo.mark_missing(project_file.id, last_seen_at="2026-03-13T12:00:00+00:00")
    missing_file = files_repo.get_by_id(project_file.id)
    assert missing_file is not None
    assert missing_file.is_missing is True


def test_rules_repository_settings_rules_and_patterns(db_session: Session) -> None:
    repo = RulesRepository(db_session)

    repo.set_setting(
        key="scheduler.mode",
        value_json='"cron"',
        updated_at="2026-03-13T10:00:00+00:00",
    )
    repo.set_setting(
        key="scheduler.mode",
        value_json='"manual"',
        updated_at="2026-03-13T11:00:00+00:00",
    )
    rule = repo.create_extension_rule(
        extension="aaf",
        enabled=True,
        max_size_bytes=104857600,
        oversize_action="skip",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
    )
    pattern = repo.create_excluded_pattern(
        pattern_type="directory_name",
        pattern_value="Cache",
        enabled=True,
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
    )

    repo.update_extension_rule(
        rule.id,
        enabled=False,
        max_size_bytes=209715200,
        oversize_action="warn",
        updated_at="2026-03-13T11:00:00+00:00",
    )
    repo.update_excluded_pattern(
        pattern.id,
        enabled=False,
        updated_at="2026-03-13T11:00:00+00:00",
    )

    setting = repo.get_setting("scheduler.mode")
    updated_rule = repo.get_extension_rule("aaf")
    patterns = repo.list_excluded_patterns()

    assert setting is not None
    assert setting.value_json == '"manual"'
    assert len(repo.list_settings()) == 1
    assert updated_rule is not None
    assert updated_rule.enabled is False
    assert updated_rule.max_size_bytes == 209715200
    assert patterns[0].enabled is False
    assert repo.list_extension_rules(enabled_only=True) == []
    assert repo.list_excluded_patterns(enabled_only=True) == []


def test_runs_repository_create_get_list_and_events(db_session: Session) -> None:
    repo = RunsRepository(db_session)

    run = repo.create_run(
        run_type="daily",
        status="running",
        started_at="2026-03-13T10:00:00+00:00",
        trigger_mode="cron",
    )
    event = repo.add_run_event(
        run_id=run.id,
        event_time="2026-03-13T10:01:00+00:00",
        level="INFO",
        event_type="scan_started",
        message="Scan started",
        payload_json='{"root_count": 1}',
    )

    repo.update_run_status(
        run.id,
        status="completed",
        finished_at="2026-03-13T10:05:00+00:00",
    )

    refreshed_run = repo.get_run(run.id)
    listed_runs = repo.list_runs()
    listed_events = repo.list_run_events(run.id)

    assert refreshed_run is not None
    assert refreshed_run.status == "completed"
    assert refreshed_run.finished_at == "2026-03-13T10:05:00+00:00"
    assert listed_runs == [refreshed_run]
    assert listed_events == [event]
