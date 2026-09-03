#!/usr/bin/env bash
# Run on comm-cme-p01 after the project tarball is extracted.
set -euo pipefail

PROJECT_DIR="${1:-$HOME/tiktok_research}"
cd "$PROJECT_DIR"

echo "=== TikTok research server setup ==="
echo "Project dir: $PROJECT_DIR"

if [[ ! -f .env ]]; then
  echo "ERROR: .env not found in $PROJECT_DIR"
  exit 1
fi

PYTHON=""
for candidate in python3.11 python3.10 python3 python; do
  if command -v "$candidate" >/dev/null 2>&1; then
    PYTHON="$candidate"
    break
  fi
done
if [[ -z "$PYTHON" ]]; then
  echo "ERROR: no Python interpreter found"
  exit 1
fi

echo "Using Python: $($PYTHON --version 2>&1) at $(command -v "$PYTHON")"

if [[ ! -d .venv ]]; then
  "$PYTHON" -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

mkdir -p data/raw audio \
  p1_content_creators/{results/{csv,parquet,summaries},logs/checkpoints,box} \
  p2_news/{results/{csv,parquet,summaries},logs/checkpoints,box} \
  p3_keywords/{results/{csv,parquet,summaries},logs/checkpoints,box}

echo ""
echo "=== Validating API connectivity ==="
python common/scripts/test_setup.py
TEST_EXIT=$?

echo ""
if [[ $TEST_EXIT -eq 0 ]]; then
  echo "Setup OK. Example commands:"
  echo "  source .venv/bin/activate"
  echo "  python common/scripts/test_setup.py"
  echo "  python p1_content_creators/scripts/run_content_creators.py --date YYYY-MM-DD --sample"
else
  echo "Setup finished with validation warnings (see data/test_setup_validation.json)."
  echo "TikTok may still work on this server even if OpenAI check failed."
fi
