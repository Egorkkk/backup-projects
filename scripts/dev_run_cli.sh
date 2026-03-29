#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"

if [ ! -x "$VENV_PYTHON" ]; then
  echo "Missing executable venv python: $VENV_PYTHON" >&2
  exit 1
fi

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT/src${PYTHONPATH+:$PYTHONPATH}" exec "$VENV_PYTHON" -c \
  'from backup_projects.cli.app import main; import sys; sys.argv[0] = "backup-projects"; raise SystemExit(main(sys.argv[1:]))' \
  "$@"
