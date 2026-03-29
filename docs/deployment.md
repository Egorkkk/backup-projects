# Deployment

## Cron Setup

v1 uses cron-ready CLI execution.

Cron should invoke [scripts/dev_run_cli.sh](/home/egorkkk/projects/backup-projects/scripts/dev_run_cli.sh).

Use the accepted example crontab lines in:
- [scripts/cron_daily.example](/home/egorkkk/projects/backup-projects/scripts/cron_daily.example)
- [scripts/cron_weekly.example](/home/egorkkk/projects/backup-projects/scripts/cron_weekly.example)

These examples assume real config files at `config/app.yaml` and `config/rules.yaml`.

The launcher script normalizes the repository root itself, so cron does not need to manage the working directory separately.

The repo-local `.venv` must exist for the launcher to work.

## Possible Later Move to systemd

v1 currently uses cron as the documented baseline.

A later move to systemd is possible as a future operational option, but it is not the current baseline.

Any future systemd setup should reuse the same accepted command surfaces rather than redefining application behavior.
