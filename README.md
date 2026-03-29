# backup-projects

`backup-projects` is a v1 backup system for project files on RAID-backed storage. It combines inventory discovery, SQLite state, policy-based manifest building, restic-backed backup flows, a CLI, and a small Flask operator UI.

## Quick start

Use the repo-local virtual environment, prepare real config files from the examples, initialize SQLite, run the daily CLI flow, and then launch the local UI.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
cp config/app.example.yaml config/app.yaml
cp config/rules.example.yaml config/rules.yaml
scripts/dev_run_cli.sh init-db --app-config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh run-daily --config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_web.sh
```

Before running the CLI flow, update `config/app.yaml` and `config/rules.yaml` for your local paths and backup settings. Deployment and cron details live in [docs/deployment.md](docs/deployment.md).

## Development setup

This project targets Python `3.12+` and expects a repo-local `.venv`.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

Start from the config templates in `config/app.example.yaml` and `config/rules.example.yaml`, then create your working `config/app.yaml` and `config/rules.yaml`.

Available local checks:

```bash
make test
make lint
```

## Run CLI

The canonical CLI surface is `scripts/dev_run_cli.sh`.

```bash
scripts/dev_run_cli.sh --help
scripts/dev_run_cli.sh init-db --app-config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh scan-roots --config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh run-daily --config config/app.yaml --rules-config config/rules.yaml
scripts/dev_run_cli.sh runs --config config/app.yaml list
```

Backup-capable flows rely on the password environment variable configured in `config/app.yaml`. The example app config currently uses `RESTIC_PASSWORD`.

## Run Flask UI

The canonical development launcher for the Flask UI is `scripts/dev_run_web.sh`.

```bash
scripts/dev_run_web.sh
```

With the example web settings, the UI starts at `http://127.0.0.1:8080/`.

This launcher currently reads `config/app.example.yaml` and `config/rules.example.yaml`, not `config/app.yaml`.

Further docs: [docs/deployment.md](docs/deployment.md)
