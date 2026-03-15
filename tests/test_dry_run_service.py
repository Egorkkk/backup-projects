from pathlib import Path

from sqlalchemy.orm import Session

from backup_projects.adapters.db.schema import create_schema
from backup_projects.adapters.db.session import (
    create_session_factory,
    create_sqlite_engine,
    session_scope,
)
from backup_projects.repositories.manual_includes_repo import ManualIncludesRepository
from backup_projects.repositories.project_dirs_repo import ProjectDirsRepository
from backup_projects.repositories.project_files_repo import ProjectFilesRepository
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.repositories.rules_repo import RulesRepository
from backup_projects.repositories.settings_repo import SettingsRepository
from backup_projects.services.dry_run_service import build_root_dry_run_manifest


def test_dry_run_service_builds_manifest_from_inventory_and_manual_includes(
    tmp_path: Path,
) -> None:
    engine = create_sqlite_engine(tmp_path / "runtime" / "db" / "dry-run.sqlite3")
    create_schema(engine)
    session_factory = create_session_factory(engine)
    root_path = (tmp_path / "root").resolve()
    root_path.mkdir()

    with session_scope(session_factory) as session:
        root = _seed_root_inventory(session=session, root_path=root_path)
        built_manifest = build_root_dry_run_manifest(session=session, root_id=root.id)

    engine.dispose()

    decisions_by_path = {
        decision.candidate.absolute_path: decision
        for decision in built_manifest.decisions
    }

    assert built_manifest.manifest_paths == (
        (root_path / "manual" / "exact.txt").as_posix(),
        (root_path / "manual" / "orphan.bin").as_posix(),
        (root_path / "project1.txt").as_posix(),
    )
    assert tuple(decisions_by_path) == (
        (root_path / "excluded" / "keep.txt").as_posix(),
        (root_path / "manual" / "exact.txt").as_posix(),
        (root_path / "manual" / "orphan.bin").as_posix(),
        (root_path / "project1.txt").as_posix(),
    )

    excluded_decision = decisions_by_path[
        (root_path / "excluded" / "keep.txt").as_posix()
    ]
    exact_decision = decisions_by_path[(root_path / "manual" / "exact.txt").as_posix()]
    forced_decision = decisions_by_path[
        (root_path / "manual" / "orphan.bin").as_posix()
    ]

    assert excluded_decision.include is False
    assert excluded_decision.reason == "excluded"
    assert excluded_decision.warning is None
    assert exact_decision.include is True
    assert exact_decision.reason == "policy_include"
    assert exact_decision.candidate.manual_include_id == 2
    assert exact_decision.manual_include_applied is True
    assert exact_decision.force_include_applied is True
    assert forced_decision.include is True
    assert forced_decision.reason == "force_include_override_policy_unknown_extension"
    assert forced_decision.manual_include_applied is True
    assert forced_decision.force_include_applied is True
    assert forced_decision.warning == "unknown_extension"

    assert built_manifest.json_payload["counts"] == {
        "total_decisions": 4,
        "included": 3,
        "skipped": 1,
        "warnings": 1,
        "included_bytes": 35,
    }
    assert built_manifest.summary_text == "\n".join(
        [
            "Manifest summary",
            "Total decisions: 4",
            "Included: 3",
            "Skipped: 1",
            "Warnings: 1",
            "Included bytes: 35",
            "",
            "Reason counts:",
            "- excluded: 1",
            "- force_include_override_policy_unknown_extension: 1",
            "- policy_include: 2",
            "",
            "Warning counts:",
            "- unknown_extension: 1",
        ]
    )


def _seed_root_inventory(*, session: Session, root_path: Path):
    roots_repo = RootsRepository(session)
    project_dirs_repo = ProjectDirsRepository(session)
    project_files_repo = ProjectFilesRepository(session)
    manual_includes_repo = ManualIncludesRepository(session)
    rules_repo = RulesRepository(session)
    settings_repo = SettingsRepository(session)

    root = roots_repo.create(
        raid_name="raid_a",
        name="root",
        path=root_path.as_posix(),
        device_id=1,
        inode=2,
        mtime_ns=3,
        ctime_ns=4,
        first_seen_at="2026-03-15T10:00:00+00:00",
        last_seen_at="2026-03-15T10:00:00+00:00",
    )
    root_dir = project_dirs_repo.create(
        root_id=root.id,
        relative_path="",
        name="root",
        dir_type="unknown",
        first_seen_at="2026-03-15T10:00:00+00:00",
        last_seen_at="2026-03-15T10:00:00+00:00",
    )
    manual_dir = project_dirs_repo.create(
        root_id=root.id,
        relative_path="manual",
        name="manual",
        dir_type="unknown",
        first_seen_at="2026-03-15T10:00:00+00:00",
        last_seen_at="2026-03-15T10:00:00+00:00",
    )
    excluded_dir = project_dirs_repo.create(
        root_id=root.id,
        relative_path="excluded",
        name="excluded",
        dir_type="unknown",
        first_seen_at="2026-03-15T10:00:00+00:00",
        last_seen_at="2026-03-15T10:00:00+00:00",
    )

    project_files_repo.create(
        project_dir_id=root_dir.id,
        relative_path="project1.txt",
        filename="project1.txt",
        extension="txt",
        size_bytes=10,
        mtime_ns=10,
        ctime_ns=9,
        inode=100,
        first_seen_at="2026-03-15T10:01:00+00:00",
        last_seen_at="2026-03-15T10:01:00+00:00",
    )
    project_files_repo.create(
        project_dir_id=manual_dir.id,
        relative_path="manual/exact.txt",
        filename="exact.txt",
        extension="txt",
        size_bytes=20,
        mtime_ns=11,
        ctime_ns=10,
        inode=101,
        first_seen_at="2026-03-15T10:01:00+00:00",
        last_seen_at="2026-03-15T10:01:00+00:00",
    )
    project_files_repo.create(
        project_dir_id=manual_dir.id,
        relative_path="manual/orphan.bin",
        filename="orphan.bin",
        extension="bin",
        size_bytes=5,
        mtime_ns=12,
        ctime_ns=11,
        inode=102,
        first_seen_at="2026-03-15T10:01:00+00:00",
        last_seen_at="2026-03-15T10:01:00+00:00",
    )
    project_files_repo.create(
        project_dir_id=excluded_dir.id,
        relative_path="excluded/keep.txt",
        filename="keep.txt",
        extension="txt",
        size_bytes=8,
        mtime_ns=13,
        ctime_ns=12,
        inode=103,
        first_seen_at="2026-03-15T10:01:00+00:00",
        last_seen_at="2026-03-15T10:01:00+00:00",
    )
    project_files_repo.create(
        project_dir_id=root_dir.id,
        relative_path="missing.txt",
        filename="missing.txt",
        extension="txt",
        size_bytes=99,
        mtime_ns=14,
        ctime_ns=13,
        inode=104,
        first_seen_at="2026-03-15T10:01:00+00:00",
        last_seen_at="2026-03-15T10:01:00+00:00",
        is_missing=True,
    )

    manual_includes_repo.create(
        root_id=root.id,
        relative_path="manual",
        include_path_type="directory",
        recursive=True,
        force_include=True,
        enabled=True,
        created_at="2026-03-15T10:02:00+00:00",
        updated_at="2026-03-15T10:02:00+00:00",
    )
    manual_includes_repo.create(
        root_id=root.id,
        relative_path="manual/exact.txt",
        include_path_type="file",
        recursive=False,
        force_include=False,
        enabled=True,
        created_at="2026-03-15T10:03:00+00:00",
        updated_at="2026-03-15T10:03:00+00:00",
    )

    rules_repo.create_extension_rule(
        extension="txt",
        enabled=True,
        max_size_bytes=None,
        oversize_action="skip",
        created_at="2026-03-15T10:04:00+00:00",
        updated_at="2026-03-15T10:04:00+00:00",
    )
    rules_repo.create_excluded_pattern(
        pattern_type="path_substring",
        pattern_value="excluded/",
        enabled=True,
        created_at="2026-03-15T10:05:00+00:00",
        updated_at="2026-03-15T10:05:00+00:00",
    )

    settings_repo.set_setting(
        key="oversize.default_action",
        value_json='"skip"',
        updated_at="2026-03-15T10:06:00+00:00",
    )
    settings_repo.set_setting(
        key="oversize.log_skipped",
        value_json="false",
        updated_at="2026-03-15T10:06:00+00:00",
    )
    settings_repo.set_setting(
        key="unknown_extensions.action",
        value_json='"collect_and_skip"',
        updated_at="2026-03-15T10:06:00+00:00",
    )
    settings_repo.set_setting(
        key="unknown_extensions.store_in_registry",
        value_json="true",
        updated_at="2026-03-15T10:06:00+00:00",
    )
    settings_repo.set_setting(
        key="unknown_extensions.log_warning",
        value_json="true",
        updated_at="2026-03-15T10:06:00+00:00",
    )

    return root
