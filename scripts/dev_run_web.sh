#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"
APP_CONFIG="$REPO_ROOT/config/app.example.yaml"
RULES_CONFIG="$REPO_ROOT/config/rules.example.yaml"

if [ ! -x "$VENV_PYTHON" ]; then
  echo "Missing executable venv python: $VENV_PYTHON" >&2
  exit 1
fi

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT/src${PYTHONPATH+:$PYTHONPATH}" exec "$VENV_PYTHON" -c '
from backup_projects.config import load_config
from backup_projects.web.app import create_app

config = load_config(app_path="config/app.example.yaml", rules_path="config/rules.example.yaml")
app = create_app(config=config)
app.run(
    host=config.app_config.web.host,
    port=config.app_config.web.port,
    debug=config.app_config.web.debug,
)
'
