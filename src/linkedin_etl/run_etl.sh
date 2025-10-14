#!/usr/bin/env bash
set -euo pipefail

WORKDIR="~/portfolio/src/linkedin_etl"         
VENV="~/portfolio/src/linkedin_etl/.venv"      
PYTHON="$VENV/bin/python"
LOGDIR="$WORKDIR/logs"
ENVFILE="$WORKDIR/.env"                     

mkdir -p "$LOGDIR"
cd "$WORKDIR"

if [[ -f "$ENVFILE" ]]; then
  set -a
  source "$ENVFILE"
  set +a
fi

source "$VENV/bin/activate"

timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
$PYTHON -m pip install -U pip >/dev/null 2>&1 || true
$PYTHON etl/main.py >> "$LOGDIR/etl_$timestamp.log" 2>&1

cd "$LOGDIR"
ls -1t etl_*.log | tail -n +21 | xargs -r rm --
