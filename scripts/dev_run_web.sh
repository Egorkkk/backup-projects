#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"
APP_CONFIG="${APP_CONFIG:-$REPO_ROOT/config/app.yaml}"
RULES_CONFIG="${RULES_CONFIG:-$REPO_ROOT/config/rules.yaml}"

if [ ! -x "$VENV_PYTHON" ]; then
  echo "Missing executable venv python: $VENV_PYTHON" >&2
  exit 1
fi

cd "$REPO_ROOT"
export APP_CONFIG RULES_CONFIG
PYTHONPATH="$REPO_ROOT/src${PYTHONPATH+:$PYTHONPATH}" exec "$VENV_PYTHON" -c '
import os

from backup_projects.config import load_config
from backup_projects.web.app import create_app

config = load_config(
    app_path=os.environ["APP_CONFIG"],
    rules_path=os.environ["RULES_CONFIG"],
)
app = create_app(config=config)
app.run(
    host=config.app_config.web.host,
    port=config.app_config.web.port,
    debug=config.app_config.web.debug,
)
'
