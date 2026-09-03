#!/usr/bin/env bash
# Append GCP enrichment settings to server .env WITHOUT overwriting TikTok secrets.
# Run ONCE on comm-cme-p01 after placing the service-account JSON.
#
# Usage (on server):
#   bash server/configure_gcp_env.sh
#   bash server/configure_gcp_env.sh /home/cme-user1/keys/tiktok-enrichment-worker.json

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/.env"
KEY="${1:-/home/cme-user1/keys/tiktok-enrichment-worker.json}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Create .env with TikTok + OpenAI keys first."
  exit 1
fi

mkdir -p "$(dirname "$KEY")"
if [[ ! -f "$KEY" ]]; then
  echo "WARNING: credentials file not found yet: $KEY"
  echo "Place tiktok-enrichment-worker.json there, then re-run."
fi

# Remove prior enrichment block if present, then append fresh defaults
tmp="$(mktemp)"
grep -vE '^(GOOGLE_APPLICATION_CREDENTIALS|GCP_PROJECT|BIGQUERY_PROJECT|BIGQUERY_DATASET|BIGQUERY_LOCATION|VISION_ENABLED|WHISPER_BACKEND|WHISPER_MODEL|WHISPER_COMPUTE_TYPE)=' \
  "$ENV_FILE" > "$tmp" || true
mv "$tmp" "$ENV_FILE"

cat >> "$ENV_FILE" <<EOF

# --- Enrichment / GCP (cfme-mediaengagment-prod) — managed by configure_gcp_env.sh ---
GOOGLE_APPLICATION_CREDENTIALS=$KEY
GCP_PROJECT=cfme-mediaengagment-prod
BIGQUERY_PROJECT=cfme-mediaengagment-prod
BIGQUERY_DATASET=tiktok_research
BIGQUERY_LOCATION=US
VISION_ENABLED=true
WHISPER_BACKEND=openai
WHISPER_MODEL=base
WHISPER_COMPUTE_TYPE=int8
EOF

chmod 600 "$ENV_FILE"
echo "Updated $ENV_FILE for GCP project cfme-mediaengagment-prod"
echo "Next:"
echo "  source .venv/bin/activate"
echo "  pip install -r requirements-enrichment.txt"
echo "  python scripts/enrich_pipeline.py --ensure-bq-schema"
