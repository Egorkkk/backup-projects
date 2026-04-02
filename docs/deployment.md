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

The canonical report artifacts remain under `runtime/reports/run-<id>/...`. Any extra report delivery configured in `app.yaml` is an additional copy step on top of those canonical artifacts, not a replacement for them.

## Restic Local Repository

The restic repository location is configured through `app.yaml`, not hardcoded in the application. The example config shows a local filesystem path, but operators may point `restic.repository` at a different local path in a real deployment. The config stores only the env-var name in `restic.password_env_var`; the actual password value must be present in the runtime environment when backup-capable flows run.

- `restic.binary`: restic executable name or path, example `restic`
- `restic.repository`: local repository path, example `/mnt/backup/restic-repo`
- `restic.password_env_var`: configurable env-var name for the password, example `RESTIC_PASSWORD`
- `restic.timeout_seconds`: backup command timeout, example `7200`
- backup-capable CLI/jobs flows consume the configured repository path and pass it to restic via `RESTIC_REPOSITORY`
- the v1 baseline assumes an already-prepared local restic repository path configured in `app.yaml`; the app consumes that path but does not define deployment-specific storage layout or repository initialization steps

The current accepted nightly `run-daily` flow uses that local repository as the primary backup target. The application does not initialize the local or remote repositories for the operator.

Optional archive settings live under `restic.archive`:
- `restic.archive.enabled`: enable post-backup archive for `run-daily`
- `restic.archive.remote_repository`: remote repository that receives the copied snapshot
- `restic.archive.remote_password_env_var`: env-var name for the remote repository password
- `restic.archive.local_retention_keep_last`: how many snapshots to keep locally after a successful archive copy

When archive is enabled, the order is:
1. `run-daily` writes the local backup snapshot to `restic.repository`
2. the exact successful snapshot is copied to `restic.archive.remote_repository`
3. only after that successful copy does local retention run against the local repository

If archive fails, local retention is skipped and the local snapshot remains intact. The current implementation does not archive snapshots for `run-backup` or `backup`.

## Report Delivery

Optional report delivery settings live under `report_delivery`:
- `report_delivery.enabled`: enable the extra delivery step
- `report_delivery.mode`: currently only `local_file`
- `report_delivery.output_dir`: destination directory for the additional delivered copy

When enabled for `run-daily`, the application writes the ordinary per-run report first under `runtime/reports/run-<id>/`, then copies the canonical `report.txt` artifact into the configured local delivery directory.

Delivery failure is non-fatal:
- the run keeps its existing final status
- the canonical report under `runtime/reports/run-<id>/` remains authoritative
- the failure is recorded in run events and logs

## Cron Setup

v1 uses cron-ready CLI execution.

Cron should invoke [scripts/dev_run_cli.sh](/home/egorkkk/projects/backup-projects/scripts/dev_run_cli.sh).

Use the accepted example crontab lines in:
- [scripts/cron_daily.example](/home/egorkkk/projects/backup-projects/scripts/cron_daily.example)
- [scripts/cron_weekly.example](/home/egorkkk/projects/backup-projects/scripts/cron_weekly.example)

These examples assume real config files at `config/app.yaml` and `config/rules.yaml`.

The launcher script normalizes the repository root itself, so cron does not need to manage the working directory separately.

The repo-local `.venv` must exist for the launcher to work.

The accepted nightly surface remains:

```sh
scripts/dev_run_cli.sh run-daily --config config/app.yaml --rules-config config/rules.yaml
```

This same `run-daily` flow may, according to config, perform:
- local backup to the configured local restic repository
- post-backup archive copy to a remote restic repository
- local retention keep-last against the local repository
- optional local-file report delivery after the ordinary report write

## Reverse Proxy Notes

The accepted current web launcher remains `scripts/dev_run_web.sh`, which starts the Flask app using `config/app.yaml` and `config/rules.yaml` by default and binds to the configured `web.host`, `web.port`, and `web.debug` values. `APP_CONFIG` and `RULES_CONFIG` may be used to override those paths when needed. Reverse proxying is an optional deployment layer on top of that same listener, not a different application mode. The example config values are `127.0.0.1`, `8080`, and `debug: false`, so a same-host reverse proxy can point at that local upstream when HTTP exposure is needed.

- the current baseline is simple root-path proxying to the configured Flask listener
- operators who change `web.host` or `web.port` in real config should point their reverse proxy upstream to that configured address
- this note is intentionally limited and does not define a vendor-specific proxy setup

## Possible Later Move to systemd

v1 currently uses cron as the documented baseline.

A later move to systemd is possible as a future operational option, but it is not the current baseline.

Any future systemd setup should reuse the same accepted command surfaces rather than redefining application behavior.
