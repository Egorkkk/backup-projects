# Architecture

This document describes the accepted repository structure and dependency boundaries for `backup-projects` v1. It is a high-level map of the active layers and the main data and job flows in the current codebase.

## Layers

The active shared config and contract layer lives in `src/backup_projects/config.py`, `src/backup_projects/constants.py`, `src/backup_projects/domain/`, and `src/backup_projects/converters.py`. These modules define validated config objects, shared enums and models, and simple conversions between repository records and domain-facing data.

The infrastructure adapter layer lives in `src/backup_projects/adapters/`. It contains SQLite/session helpers, filesystem helpers, process helpers, and the restic adapter used by backup flows.

The repository layer lives in `src/backup_projects/repositories/`. Repositories own SQLAlchemy table access and SQLite read/write operations for roots, project dirs, project files, rules, settings, manual includes, and runs.

The service layer lives in `src/backup_projects/services/`. Services contain application logic such as root discovery, structural and incremental scan logic, dry-run manifest building, backup execution, reporting, run visibility, dashboard data assembly, and web/CLI-facing view-building helpers.

The job layer lives in `src/backup_projects/jobs/`. Jobs coordinate multi-step pipelines such as scan, backup, and daily runs using services, repositories, config, and adapters.

The CLI entry layer lives in `src/backup_projects/cli/`. CLI modules parse arguments, load config, open sessions, call the appropriate services, repositories, or jobs, and format terminal output.

The web entry layer lives in `src/backup_projects/web/`. It contains the Flask app factory, route registration, templates, and static assets for the operator UI.

In simple terms, web and CLI are entry surfaces, jobs orchestrate multi-step runs, services hold application logic, repositories own SQL access, and adapters handle external integration details. The accepted boundaries are:

- web does not issue SQL directly
- cli does not issue SQL directly
- jobs do not know HTML or templates
- services do not know Flask request/response objects
- new feature behavior should land in CLI/service seams before Web UI

The current codebase is not stricter than that baseline. Web routes delegate to services and session helpers. CLI modules often delegate to services, but some CLI modules also use repositories directly as long as they still avoid raw SQL and schema access.

## Data Flow

Configuration enters through `src/backup_projects/config.py`. YAML files are loaded into a validated `ProjectConfig`, and that config object is then passed into CLI entrypoints, the Flask app factory, services, and jobs.

The common read flow starts with an entry surface opening an SQLite session through `src/backup_projects/adapters/db/session.py`. A service or view-builder then reads data through repositories, and the entry surface formats the result either for terminal output or for HTML rendering.

The roots flow is the clearest example of that pattern. `src/backup_projects/web/routes_roots.py` and `src/backup_projects/cli/roots.py` both open a session and delegate to `src/backup_projects/services/roots_service.py`, which reads from the roots repository and returns a view-oriented result. The web route renders a template, while the CLI module prints a terminal table-like listing.

The runs flow follows the same shape with an extra service seam. `src/backup_projects/web/routes_runs.py` calls `src/backup_projects/services/runs_service.py`, which builds web-facing history and details views on top of `src/backup_projects/services/run_visibility_service.py`. The lower visibility service reads run rows, events, reports, and log-file status, while the higher service adapts that data for the web layer.

Write and orchestration flows use the same boundaries. Inventory and scan operations update SQLite through repositories rather than direct SQL in entry surfaces. Dry-run logic reads inventory and policy state, evaluates candidates, and builds manifest artifacts through `src/backup_projects/services/dry_run_service.py` and `src/backup_projects/services/manifest_builder.py`. Backup execution uses `src/backup_projects/services/backup_service.py`, which delegates the external restic call to `src/backup_projects/adapters/restic_adapter.py`.

Run metadata, lock state, logs, and reports move through dedicated services rather than through templates or CLI code directly. The main seams are `src/backup_projects/services/run_service.py`, `src/backup_projects/services/run_lock.py`, `src/backup_projects/services/logging_setup.py`, and `src/backup_projects/services/report_service.py`.

## Jobs Flow

The accepted jobs share a common orchestration pattern. A job starts a run record, acquires a lock, configures run logging, resolves active roots or other inputs, performs its pipeline steps, writes summary and report artifacts, and then finishes the run with a final status.

`src/backup_projects/jobs/scan_job.py` is the scan-oriented orchestration layer. It performs root discovery, runs structural rescans when required, performs incremental project-dir scanning, applies manual includes, and writes run summary and report artifacts.

`src/backup_projects/jobs/backup_job.py` is the backup-oriented orchestration layer. It resolves active roots, builds dry-run manifests, writes manifest artifacts, invokes the backup service/restic flow, and then records summary and report output for the run.

`src/backup_projects/jobs/daily_job.py` is the combined daily pipeline. It performs root discovery, structural rescan when needed, incremental scan, manual-include processing, manifest generation, backup execution, and final reporting as one end-to-end daily run.

The current public trigger surfaces for these orchestration flows are intentionally limited. The main CLI job trigger is `src/backup_projects/cli/run_daily.py`. The web UI can also trigger selected operator actions through `src/backup_projects/web/routes_actions.py`, which delegates to `src/backup_projects/services/actions_service.py` for manual daily runs, backup runs, root dry-runs, and root rescans.

`run-weekly` exists as a placeholder CLI entrypoint, but it is not an implemented job flow in the current v1 baseline and is not part of the accepted architecture flow described here.

Out of scope here: policy details, deployment and runtime-path operations, the full CLI command catalog, and a page-by-page Web UI guide.
