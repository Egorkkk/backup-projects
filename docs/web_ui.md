# Web UI

This document describes the accepted current Flask UI surface and a few typical operator scenarios for the v1 baseline.

## Start the UI

The canonical launcher is `scripts/dev_run_web.sh`.

That launcher currently loads by default:
- `config/app.yaml`
- `config/rules.yaml`

If needed, `APP_CONFIG` and `RULES_CONFIG` can override those paths.

The example config values are `http://127.0.0.1:8080/` with `debug: false`.

## Sections

- Dashboard `/`
  - Summary page with last scan, last backup, run status, counts, and skipped oversized summary.
  - Includes the `Run daily now` and `Backup now` actions.

- Roots `/roots`
  - Lists roots with status and rescan filters.
  - Active roots expose `Dry-run now` and `Rescan root` actions.

- Project Dirs `/dirs`
  - Lists known project directories.

- Rules `/rules`
  - Shows the default `.aaf` size limit.
  - Supports creating and updating extension rules.
  - Supports creating and toggling excluded patterns.
  - Validation errors re-render on the same page.

- Includes `/includes`
  - Supports creating manual include entries.
  - Lists existing manual includes.
  - Supports enable/disable and delete actions.
  - The visible form includes `force include` and `recursive`.

- Runs `/runs`
  - Lists recorded runs.
  - Links to run details.

- Run Details `/runs/<id>`
  - Shows run metadata and events.
  - Shows an artifacts table.
  - Download links are available only for existing `json`, `text`, and `html` report artifacts.

- Review pages
  - `/review/oversized-skipped`
  - `/review/unrecognized-extensions`
  - `/review/manual-overrides`
  - These are read-only review lists.

- Action result page
  - POST actions render a result page with status, message, optional fields/details, and a back link.

## Typical Scenarios

- Check current status and trigger an operator action
  - Open `/` to inspect the last scan, last backup, current run status, and current counts.
  - Use `Run daily now` or `Backup now` and review the resulting action page.

- Inspect and rescan a root
  - Open `/roots`, apply status or rescan filters, and inspect the listed roots.
  - Use `Dry-run now` or `Rescan root` for an active root and review the result page.

- Adjust rules or manual includes
  - Open `/rules` to create or update an extension rule or excluded pattern.
  - Open `/includes` to create, enable/disable, or delete a manual include entry.

- Review what happened in recent runs
  - Open `/runs` to inspect recent runs.
  - Open `/runs/<id>` to review events and available report artifacts.

- Review special-case policy outcomes
  - Use `/review/oversized-skipped` for oversized skipped files.
  - Use `/review/unrecognized-extensions` for collected unknown extensions.
  - Use `/review/manual-overrides` for manual-override cases.

Out of scope here: deployment and reverse-proxy guidance, CLI reference, deep architecture explanation, detailed field-by-field policy semantics, and aspirational future pages or workflows.
