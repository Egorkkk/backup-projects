# Deployment

## Runtime Paths

Runtime paths are configured through `app.yaml`, not hardcoded only to repo-root `runtime/*`. The example config uses repo-local defaults, but operators may point these paths elsewhere in a real deployment. Relative runtime paths and `db.sqlite_path` are resolved relative to the directory containing the app config file.

The current example defaults are:
- `runtime.logs_dir` -> `runtime/logs`
- `runtime.manifests_dir` -> `runtime/manifests`
- `runtime.reports_dir` -> `runtime/reports`
- `runtime.db_dir` -> `runtime/db`
- `runtime.locks_dir` -> `runtime/locks`
- `db.sqlite_path` -> `runtime/db/backup_projects.sqlite3`

Established runtime file conventions inside those base directories are:
- lock file: `runtime/locks/run.lock`
- per-run log file: `runtime/logs/run-<id>/run.log`
- per-run reports: `runtime/reports/run-<id>/report.json`, `runtime/reports/run-<id>/report.txt`, `runtime/reports/run-<id>/report.html`
- manifest outputs: files written under `runtime/manifests/`

The repo already carries these runtime directories as part of the baseline layout. Code also creates or validates specific runtime dirs and files during execution, so operators should treat the configured paths as writable application state.

## Restic Local Repository

The restic repository location is configured through `app.yaml`, not hardcoded in the application. The example config shows a local filesystem path, but operators may point `restic.repository` at a different local path in a real deployment. The config stores only the env-var name in `restic.password_env_var`; the actual password value must be present in the runtime environment when backup-capable flows run.

- `restic.binary`: restic executable name or path, example `restic`
- `restic.repository`: local repository path, example `/mnt/backup/restic-repo`
- `restic.password_env_var`: configurable env-var name for the password, example `RESTIC_PASSWORD`
- `restic.timeout_seconds`: backup command timeout, example `7200`
- backup-capable CLI/jobs flows consume the configured repository path and pass it to restic via `RESTIC_REPOSITORY`
- the v1 baseline assumes an already-prepared local restic repository path configured in `app.yaml`; the app consumes that path but does not define deployment-specific storage layout or repository initialization steps

## Cron Setup

v1 uses cron-ready CLI execution.

Cron should invoke [scripts/dev_run_cli.sh](/home/egorkkk/projects/backup-projects/scripts/dev_run_cli.sh).

Use the accepted example crontab lines in:
- [scripts/cron_daily.example](/home/egorkkk/projects/backup-projects/scripts/cron_daily.example)
- [scripts/cron_weekly.example](/home/egorkkk/projects/backup-projects/scripts/cron_weekly.example)

These examples assume real config files at `config/app.yaml` and `config/rules.yaml`.

The launcher script normalizes the repository root itself, so cron does not need to manage the working directory separately.

The repo-local `.venv` must exist for the launcher to work.

## Reverse Proxy Notes

The accepted current web launcher remains `scripts/dev_run_web.sh`, which starts the Flask app using the configured `web.host`, `web.port`, and `web.debug` values. Reverse proxying is an optional deployment layer on top of that same listener, not a different application mode. The example config uses `127.0.0.1`, `8080`, and `debug: false`, so a same-host reverse proxy can point at that local upstream when HTTP exposure is needed.

- the current baseline is simple root-path proxying to the configured Flask listener
- operators who change `web.host` or `web.port` in real config should point their reverse proxy upstream to that configured address
- this note is intentionally limited and does not define a vendor-specific proxy setup

## Possible Later Move to systemd

v1 currently uses cron as the documented baseline.

A later move to systemd is possible as a future operational option, but it is not the current baseline.

Any future systemd setup should reuse the same accepted command surfaces rather than redefining application behavior.
