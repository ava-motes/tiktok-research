#!/usr/bin/env bash
# Run on your Mac to package and upload the project to comm-cme-p01.
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-cme-user1}"
REMOTE_HOST="${REMOTE_HOST:-comm-cme-p01.moody.utexas.edu}"
REMOTE_DIR="${REMOTE_DIR:-~/tiktok_research}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ARCHIVE="/tmp/tiktok-research-deploy.tar.gz"

echo "=== Packaging project ==="
tar -czf "$ARCHIVE" \
  --exclude='.venv' \
  --exclude='venv' \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='audio/*.mp4' \
  --exclude='audio/*.part' \
  -C "$PROJECT_DIR" .

ls -lh "$ARCHIVE"

echo ""
echo "=== Uploading to ${REMOTE_USER}@${REMOTE_HOST} ==="
ssh "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ${REMOTE_DIR}"
scp "$ARCHIVE" "${REMOTE_USER}@${REMOTE_HOST}:/tmp/tiktok-research-deploy.tar.gz"

echo ""
echo "=== Extracting and running setup on server ==="
ssh "${REMOTE_USER}@${REMOTE_HOST}" bash -s <<EOF
set -euo pipefail
mkdir -p ${REMOTE_DIR}
tar -xzf /tmp/tiktok-research-deploy.tar.gz -C ${REMOTE_DIR}
chmod +x ${REMOTE_DIR}/server/setup.sh
${REMOTE_DIR}/server/setup.sh ${REMOTE_DIR}
EOF

echo ""
echo "=== Deploy complete ==="
echo "SSH in:  ssh ${REMOTE_USER}@${REMOTE_HOST}"
echo "Work in: cd ${REMOTE_DIR} && source .venv/bin/activate"
