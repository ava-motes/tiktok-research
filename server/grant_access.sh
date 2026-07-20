#!/usr/bin/env bash
# One-time: authorize this Mac's SSH key on comm-cme-p01, then finish project setup.
# After this, use:  ssh cme-p01
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-cme-user1}"
REMOTE_HOST="${REMOTE_HOST:-comm-cme-p01.moody.utexas.edu}"
REMOTE_DIR="${REMOTE_DIR:-tiktok_research}"
PUBKEY_FILE="${HOME}/.ssh/id_ed25519.pub"
ARCHIVE="/tmp/tiktok-research-deploy.tar.gz"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ ! -f "$PUBKEY_FILE" ]]; then
  echo "ERROR: Missing $PUBKEY_FILE"
  exit 1
fi
PUBKEY="$(cat "$PUBKEY_FILE")"

if [[ ! -f "$ARCHIVE" ]]; then
  echo "Building deploy archive..."
  tar -czf "$ARCHIVE" \
    --exclude='.venv' --exclude='venv' --exclude='.git' \
    --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='audio/*.mp4' --exclude='audio/*.part' \
    -C "$PROJECT_DIR" .
fi

if [[ -z "${CME_PASSWORD:-}" ]]; then
  if [[ -t 0 ]]; then
    printf "Enter CME password for %s@%s (one time only): " "$REMOTE_USER" "$REMOTE_HOST"
    read -rs CME_PASSWORD
    echo
  else
    CME_PASSWORD=$(osascript 2>/dev/null <<'APPLESCRIPT' || true
display dialog "Enter password for cme-user1@comm-cme-p01 (one time only):" default answer "" with hidden answer buttons {"Cancel", "OK"} default button "OK"
text returned of result
APPLESCRIPT
)
  fi
  export CME_PASSWORD
fi

if [[ -z "${CME_PASSWORD:-}" ]]; then
  echo "ERROR: Password required."
  exit 1
fi

export PUBKEY REMOTE_USER REMOTE_HOST REMOTE_DIR ARCHIVE

echo "=== Step 1/4: Authorizing SSH key on server ==="
/usr/bin/expect <<'EXPECT_EOF'
set timeout 120
set password $env(CME_PASSWORD)
set pubkey $env(PUBKEY)
set remote_user $env(REMOTE_USER)
set remote_host $env(REMOTE_HOST)

spawn ssh -o StrictHostKeyChecking=accept-new ${remote_user}@${remote_host} bash -lc "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && grep -qF \"$pubkey\" ~/.ssh/authorized_keys || printf '%s\\n' \"$pubkey\" >> ~/.ssh/authorized_keys && echo KEY_OK"
expect {
    -re "(?i)password:" { send "$password\r"; exp_continue }
    "KEY_OK" { }
    "Permission denied" { puts "\nERROR: bad password"; exit 1 }
    timeout { puts "\nERROR: timed out"; exit 1 }
}
expect eof
EXPECT_EOF

echo "=== Step 2/4: Verifying passwordless SSH ==="
ssh -o BatchMode=yes -o ConnectTimeout=15 "${REMOTE_USER}@${REMOTE_HOST}" "echo SSH_OK && hostname"

echo "=== Step 3/4: Uploading project (if needed) ==="
if ! ssh -o BatchMode=yes "${REMOTE_USER}@${REMOTE_HOST}" "test -f ~/${REMOTE_DIR}/.env"; then
  scp -o BatchMode=yes "$ARCHIVE" "${REMOTE_USER}@${REMOTE_HOST}:/tmp/tiktok-research-deploy.tar.gz"
  ssh -o BatchMode=yes "${REMOTE_USER}@${REMOTE_HOST}" \
    "mkdir -p ~/${REMOTE_DIR} && tar -xzf /tmp/tiktok-research-deploy.tar.gz -C ~/${REMOTE_DIR}"
else
  echo "Project already present on server."
fi

echo "=== Step 4/4: Running server setup ==="
ssh -o BatchMode=yes "${REMOTE_USER}@${REMOTE_HOST}" \
  "chmod +x ~/${REMOTE_DIR}/server/setup.sh && ~/${REMOTE_DIR}/server/setup.sh ~/${REMOTE_DIR}"

unset CME_PASSWORD

echo ""
echo "=== Ready ==="
echo "  ssh cme-p01"
echo "  ssh cme-p01 'cd ~/tiktok_research && source .venv/bin/activate && python scripts/test_setup.py'"
