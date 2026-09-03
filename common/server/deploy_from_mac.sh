#!/usr/bin/env bash
# Run on your Mac to package and upload the project to comm-cme-p01.
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-cme-user1}"
REMOTE_HOST="${REMOTE_HOST:-comm-cme-p01.moody.utexas.edu}"
REMOTE_DIR="${REMOTE_DIR:-~/tiktok_research}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARCHIVE="/tmp/tiktok-research-deploy.tar.gz"

echo "=== Packaging project ==="
tar -czf "$ARCHIVE" \
  --exclude='.venv' \
  --exclude='venv' \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='.env.local' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='data' \
  --exclude='audio' \
  --exclude='*.db' \
  --exclude='box_oauth.json' \
  --exclude='*service-account*.json' \
  --exclude='credentials.json' \
  -C "$PROJECT_DIR" .

ls -lh "$ARCHIVE"

echo ""
echo "=== Uploading to ${REMOTE_USER}@${REMOTE_HOST} ==="
ssh "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ${REMOTE_DIR}"
scp "$ARCHIVE" "${REMOTE_USER}@${REMOTE_HOST}:/tmp/tiktok-research-deploy.tar.gz"

echo ""
echo "=== Extracting on server (does not overwrite existing .env) ==="
ssh "${REMOTE_USER}@${REMOTE_HOST}" bash -s <<EOF
set -euo pipefail
mkdir -p ${REMOTE_DIR}
if [[ -f ${REMOTE_DIR}/.env ]]; then
  cp -p ${REMOTE_DIR}/.env /tmp/tiktok_research.env.bak
fi
tar -xzf /tmp/tiktok-research-deploy.tar.gz -C ${REMOTE_DIR}
if [[ -f /tmp/tiktok_research.env.bak ]]; then
  mv /tmp/tiktok_research.env.bak ${REMOTE_DIR}/.env
fi
chmod +x ${REMOTE_DIR}/common/server/setup.sh
echo "Extracted to ${REMOTE_DIR}. Existing .env preserved."
EOF

echo ""
echo "=== Deploy complete ==="
echo "SSH in:  ssh ${REMOTE_USER}@${REMOTE_HOST}"
echo "Work in: cd ${REMOTE_DIR} && source .venv/bin/activate"
