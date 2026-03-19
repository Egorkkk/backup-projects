from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent

from backup_projects.adapters.db.bootstrap import initialize_database
from backup_projects.adapters.db.session import (
    create_engine_from_config,
    create_session_factory,
    session_scope,
)
from backup_projects.config import load_config
from backup_projects.domain import ManifestResult
from backup_projects.repositories.roots_repo import RootsRepository
from backup_projects.repositories.runs_repo import RunsRepository
from backup_projects.services.backup_service import BackupServiceResult
from backup_projects.adapters.restic_adapter import ResticBackupResult

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_run_daily_double_run_protection_marks_second_run_locked(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    app_path, rules_path = _write_config_files(tmp_path)
    config = load_config(app_path=app_path, rules_path=rules_path)
    initialize_database(config)
    _seed_active_root(config=config, root_path=tmp_path / "raid_a" / "show-a")

    first_entered_path = tmp_path / "first-entered.txt"
    release_path = tmp_path / "release-first-run.txt"
    first_marker_path = tmp_path / "first-protected-marker.txt"
    second_marker_path = tmp_path / "second-protected-marker.txt"
    helper_script_path = _write_first_run_holder_script(tmp_path)

    env = os.environ.copy()
    python_path = str(PROJECT_ROOT / "src")
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{python_path}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = python_path

    first_run = subprocess.Popen(
        [
            sys.executable,
            str(helper_script_path),
            str(PROJECT_ROOT),
            str(app_path),
            str(rules_path),
            str(first_entered_path),
            str(release_path),
            str(first_marker_path),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_path(first_entered_path)

        from backup_projects.jobs import daily_job as daily_job_module
        from backup_projects.cli import run_daily as run_daily_module

        def second_run_marker(*, session, root, config, manifests_dir, run_timestamp):
            second_marker_path.write_text("second-run-entered", encoding="utf-8")
            return _build_fake_backup_outputs(root_id=root.id, manifests_dir=manifests_dir)

        monkeypatch.setattr(daily_job_module, "_run_backup_for_root", second_run_marker)

        exit_code = run_daily_module.main(
            [
                "--config",
                str(app_path),
                "--rules-config",
                str(rules_path),
            ]
        )

        captured = capsys.readouterr()
        assert exit_code == 0
        assert "Daily run locked for run-id:" in captured.out
        assert first_marker_path.exists()
        assert not second_marker_path.exists()

        locked_statuses = [run.status for run in _list_runs(config=config)]
        assert "locked" in locked_statuses

        release_path.write_text("release", encoding="utf-8")
        stdout, stderr = first_run.communicate(timeout=10)
        assert first_run.returncode == 0, stdout + stderr

        final_statuses = [run.status for run in _list_runs(config=config)]
        assert final_statuses.count("locked") == 1
        assert final_statuses.count("completed") == 1
    finally:
        if first_run.poll() is None:
            release_path.write_text("release", encoding="utf-8")
            first_run.kill()
            first_run.communicate(timeout=5)


def _write_config_files(tmp_path: Path) -> tuple[Path, Path]:
    app_path = tmp_path / "app.yaml"
    rules_path = tmp_path / "rules.yaml"
    raid_root_path = (tmp_path / "raid_a").as_posix()

    app_path.write_text(
        dedent(
            f"""
            app:
              name: "backup-projects"
              env: "test"
              log_level: "INFO"
            raid_roots:
              - name: "raid_a"
                path: "{raid_root_path}"
                enabled: true
            runtime:
              logs_dir: "runtime/logs"
              manifests_dir: "runtime/manifests"
              reports_dir: "runtime/reports"
              db_dir: "runtime/db"
              locks_dir: "runtime/locks"
            web:
              host: "127.0.0.1"
              port: 8080
              debug: false
            db:
              driver: "sqlite"
              sqlite_path: "runtime/db/locking.sqlite3"
            restic:
              binary: "restic"
              repository: "/mnt/backup/restic-repo"
              password_env_var: "RESTIC_PASSWORD"
              timeout_seconds: 7200
            scheduler:
              mode: "cron"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    rules_path.write_text(
        dedent(
            """
            allowed_extensions:
              - "prproj"
              - "aaf"
            size_limits:
              default_max_size_bytes: null
              by_extension:
                aaf: 104857600
            oversize:
              default_action: "warn"
              aaf_action: "skip"
              log_skipped: true
            exclude_patterns:
              directory_names:
                - "Cache"
              glob_patterns:
                - "**/.cache/**"
              path_substrings:
                - "Autosave"
            unknown_extensions:
              action: "collect_and_skip"
              store_in_registry: true
              log_warning: true
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    return app_path, rules_path


def _seed_active_root(*, config, root_path: Path) -> None:
    root_path.mkdir(parents=True, exist_ok=True)
    _prepare_runtime_dirs(config=config)
    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            RootsRepository(session).create(
                raid_name="raid_a",
                name="show-a",
                path=root_path.as_posix(),
                device_id=None,
                inode=None,
                mtime_ns=None,
                ctime_ns=None,
                first_seen_at="2026-03-20T10:00:00+00:00",
                last_seen_at="2026-03-20T10:00:00+00:00",
            )
    finally:
        engine.dispose()


def _prepare_runtime_dirs(*, config) -> None:
    runtime_root = config.app_path.parent / "runtime"
    for relative in ("logs", "manifests", "reports", "locks"):
        (runtime_root / relative).mkdir(parents=True, exist_ok=True)


def _list_runs(*, config):
    engine = create_engine_from_config(config)
    session_factory = create_session_factory(engine)
    try:
        with session_scope(session_factory) as session:
            return RunsRepository(session).list_runs(limit=10)
    finally:
        engine.dispose()


def _wait_for_path(path: Path, *, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for path: {path}")


def _build_fake_backup_outputs(*, root_id: int, manifests_dir: Path):
    manifest_result = ManifestResult(
        manifest_paths=(),
        decisions=(),
        manifest_file_path=str(manifests_dir / f"daily-held-root-{root_id}.manifest.txt"),
        json_manifest_file_path=str(manifests_dir / f"daily-held-root-{root_id}.manifest.json"),
        summary_file_path=str(manifests_dir / f"daily-held-root-{root_id}.summary.txt"),
    )
    backup_result = BackupServiceResult(
        manifest_result=manifest_result,
        restic_result=ResticBackupResult(
            manifest_file_path=manifest_result.manifest_file_path,
            snapshot_id=f"snapshot-{root_id}",
            summary_payload={"snapshot_id": f"snapshot-{root_id}"},
            argv=("restic", "backup"),
            stdout="",
            stderr="",
            duration_seconds=0.1,
        ),
    )
    return manifest_result, backup_result


def _write_first_run_holder_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "hold_first_daily_run.py"
    script_path.write_text(
        dedent(
            """
            from __future__ import annotations

            import sys
            import time
            from pathlib import Path

            project_root = Path(sys.argv[1])
            app_path = Path(sys.argv[2])
            rules_path = Path(sys.argv[3])
            entered_path = Path(sys.argv[4])
            release_path = Path(sys.argv[5])
            marker_path = Path(sys.argv[6])

            sys.path.insert(0, str(project_root / "src"))

            from backup_projects.cli import run_daily as run_daily_module
            from backup_projects.jobs import daily_job as daily_job_module
            from backup_projects.domain import ManifestResult
            from backup_projects.services.backup_service import BackupServiceResult
            from backup_projects.adapters.restic_adapter import ResticBackupResult

            def hold_protected_body(*, session, root, config, manifests_dir, run_timestamp):
                marker_path.write_text("first-run-entered", encoding="utf-8")
                entered_path.write_text("entered", encoding="utf-8")
                while not release_path.exists():
                    time.sleep(0.05)
                manifest_result = ManifestResult(
                    manifest_paths=(),
                    decisions=(),
                    manifest_file_path=str(manifests_dir / f"daily-held-root-{root.id}.manifest.txt"),
                    json_manifest_file_path=str(manifests_dir / f"daily-held-root-{root.id}.manifest.json"),
                    summary_file_path=str(manifests_dir / f"daily-held-root-{root.id}.summary.txt"),
                )
                backup_result = BackupServiceResult(
                    manifest_result=manifest_result,
                    restic_result=ResticBackupResult(
                        manifest_file_path=manifest_result.manifest_file_path,
                        snapshot_id=f"snapshot-{root.id}",
                        summary_payload={"snapshot_id": f"snapshot-{root.id}"},
                        argv=("restic", "backup"),
                        stdout="",
                        stderr="",
                        duration_seconds=0.1,
                    ),
                )
                return manifest_result, backup_result

            daily_job_module._run_backup_for_root = hold_protected_body

            raise SystemExit(
                run_daily_module.main(
                    [
                        "--config",
                        str(app_path),
                        "--rules-config",
                        str(rules_path),
                    ]
                )
            )
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path
