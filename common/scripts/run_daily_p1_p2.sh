#!/bin/bash
# Production P1 → P2 daily job on comm-cme-p01.
#
# This script is the scheduled entrypoint. Do not put a clock time here.
# When a run time is provided, only install/enable the timer or crontab —
# do not change this file's pipeline flags.
#
#   bash common/scripts/run_daily_p1_p2.sh --preflight
#   bash common/scripts/run_daily_p1_p2.sh
#   DATE=YYYY-MM-DD bash common/scripts/run_daily_p1_p2.sh
#
# DATE unset → America/Chicago today minus RESEARCH_LAG_DAYS (default 2).
# Never runs P3. Skips Whisper. No --reset-checkpoints.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
cd "$ROOT"

HOST="$(hostname | tr '[:upper:]' '[:lower:]')"
PREFLIGHT=0
for arg in "$@"; do
  if [[ "$arg" == "--preflight" ]]; then
    PREFLIGHT=1
  fi
done

if [[ "$PREFLIGHT" != "1" ]]; then
  if [[ "$HOST" != *cme-p01* ]]; then
    echo "Refusing TikTok collection/enrichment on host $(hostname)." >&2
    echo "Run on comm-cme-p01 only (Mac is for code/SSH)." >&2
    exit 1
  fi
  if [[ ! -d .venv ]]; then
    echo "STOP: missing $ROOT/.venv" >&2
    exit 2
  fi
  if [[ ! -f .env ]]; then
    echo "STOP: missing $ROOT/.env (keep the existing server file; do not copy from git)" >&2
    exit 2
  fi
fi

if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export PATH="$HOME/bin:$PATH"
export PYTHONUNBUFFERED=1

exec python common/scripts/run_daily_p1_p2.py "$@"
