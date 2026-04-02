# CLI

This document describes the accepted command-line surface for the current v1 baseline and gives a small set of representative usage examples based on the real registered CLI.

## Canonical Entrypoint

The canonical launcher is `scripts/dev_run_cli.sh`.

## Command List

Registered top-level commands:
- `init-db`: initialize the SQLite schema
- `seed-default-rules`: seed default settings and policy rules
- `scan-roots`: discover configured RAID roots and sync them to SQLite
- `scan-structure`: run structural scan for one known root
- `scan-project-dirs`: run incremental scan for known project dirs
- `scan-manual`: apply manual includes for one known root
- `run-daily`: run the daily backup pipeline for all active roots, including optional post-backup archive/retention and optional local-file report delivery according to `app.yaml`
- `backup`: build manifest artifacts and run backup for one root
- `dry-run`: simulate current policy selection without running backup
- `rules`: list and mutate policy rules in SQLite
- `include`: create, list, enable, and disable manual includes
- `runs`: list, inspect, and export recorded runs
- `roots`: list known roots
- `dirs`: list known project directories
- `files`: inspect current file visibility decisions

Grouped commands:
- `rules`: `list`, `add-extension`, `update-extension`, `add-exclude`, `disable-exclude`
- `include`: `add-file`, `add-dir`, `list`, `disable`, `enable`
- `runs`: `list`, `show`, `export`
- `roots`: `list`
- `dirs`: `list`
- `files`: `list-skipped`

`run-weekly` and `doctor` are currently placeholder entrypoints, not working v1 operational flows.

## Usage Examples

```sh
scripts/dev_run_cli.sh --help

scripts/dev_run_cli.sh init-db --app-config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh seed-default-rules --app-config config/app.yaml --rules-config config/rules.yaml

scripts/dev_run_cli.sh scan-roots --config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh scan-structure --config config/app.yaml --rules-config config/rules.yaml --root-id 1
scripts/dev_run_cli.sh scan-project-dirs --config config/app.yaml --rules-config config/rules.yaml --root-id 1
scripts/dev_run_cli.sh scan-manual --config config/app.yaml --rules-config config/rules.yaml --root-id 1

scripts/dev_run_cli.sh dry-run --config config/app.yaml --rules-config config/rules.yaml --root-id 1
scripts/dev_run_cli.sh backup --config config/app.yaml --rules-config config/rules.yaml --root-id 1 --output-dir runtime/manifests --artifact-stem manual-backup
scripts/dev_run_cli.sh run-daily --config config/app.yaml --rules-config config/rules.yaml

scripts/dev_run_cli.sh roots --config config/app.yaml list
scripts/dev_run_cli.sh dirs --config config/app.yaml list
scripts/dev_run_cli.sh files --config config/app.yaml list-skipped --root-id 1

scripts/dev_run_cli.sh rules --config config/app.yaml list
scripts/dev_run_cli.sh rules --config config/app.yaml add-extension aaf --oversize-action warn --max-size-bytes 104857600

scripts/dev_run_cli.sh include --config config/app.yaml add-file --root-id 1 path/to/file.prproj
scripts/dev_run_cli.sh include --config config/app.yaml list --root-id 1

scripts/dev_run_cli.sh runs --config config/app.yaml list
scripts/dev_run_cli.sh runs --config config/app.yaml show --run-id 1
scripts/dev_run_cli.sh runs --config config/app.yaml export --id 1 > run-1.html
```

Backup-capable commands require the restic password environment variable named in `app.yaml`.

`run-daily` remains the canonical cron-ready nightly surface. If `restic.archive.*` is enabled, it may archive the successful local snapshot to a remote repository and then apply local retention. If `report_delivery.*` is enabled, it may also copy the canonical `report.txt` artifact to a configured local output directory. These extra steps are not currently part of `run-backup` or `backup`.

`run-weekly` and `doctor` are intentionally omitted from normal workflow examples because they are placeholders.

Out of scope here: deeper policy semantics, deployment/cron behavior, Web UI workflows, and exhaustive output samples or a full flag reference.
