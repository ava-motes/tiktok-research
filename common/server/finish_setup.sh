#!/usr/bin/env bash
# Finish server setup when archive is already uploaded.
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-cme-user1}"
REMOTE_HOST="${REMOTE_HOST:-comm-cme-p01.moody.utexas.edu}"
REMOTE_DIR="${REMOTE_DIR:-tiktok_research}"
PUBKEY='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHIvlLKcbyfhE6DRZ6LzsnI62l4bZG0XBjolx0n6AOmN sivanaraharisetty@users.noreply.github.com'

if [[ -z "${CME_PASSWORD:-}" ]]; then
  if [[ -t 0 ]]; then
    printf "CME server password for %s@%s: " "$REMOTE_USER" "$REMOTE_HOST"
    read -rs CME_PASSWORD
    echo
  else
    CME_PASSWORD=$(osascript -e 'display dialog "Enter password for cme-user1@comm-cme-p01:" default answer "" with hidden answer' -e 'text returned of result' 2>/dev/null || true)
  fi
  export CME_PASSWORD
fi

if [[ -z "${CME_PASSWORD:-}" ]]; then
  echo "ERROR: No password provided."
  exit 1
fi

export PUBKEY REMOTE_USER REMOTE_HOST REMOTE_DIR

/usr/bin/expect <<'EXPECT_EOF'
set timeout 900
set password $env(CME_PASSWORD)
set pubkey $env(PUBKEY)
set remote_user $env(REMOTE_USER)
set remote_host $env(REMOTE_HOST)
set remote_dir $env(REMOTE_DIR)

puts "=== Setting up project on server ==="
spawn ssh -o StrictHostKeyChecking=accept-new ${remote_user}@${remote_host} bash -s
expect {
    -re "(?i)password:" {
        send "$password\r"
        exp_continue
    }
    -re "Permission denied" {
        puts "\nERROR: ssh authentication failed."
        exit 1
    }
    -re "\\\$|%" {}
}

send "set -euo pipefail\r"
send "mkdir -p ~/$remote_dir ~/.ssh\r"
send "chmod 700 ~/.ssh\r"
send "touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys\r"
send "grep -qF '$pubkey' ~/.ssh/authorized_keys 2>/dev/null || echo '$pubkey' >> ~/.ssh/authorized_keys\r"
send "mkdir -p ~/$remote_dir\r"
send "tar -xzf /tmp/tiktok-research-deploy.tar.gz -C ~/$remote_dir\r"
send "chmod +x ~/$remote_dir/server/setup.sh\r"
send "~/$remote_dir/server/setup.sh ~/$remote_dir\r"
send "exit\r"
expect eof
catch wait ssh_result
if {[lindex $ssh_result 3] != 0} {
    puts "ERROR: remote setup failed (exit [lindex $ssh_result 3])"
    exit 1
}
EXPECT_EOF

unset CME_PASSWORD

echo ""
echo "=== Verifying passwordless SSH ==="
ssh -o BatchMode=yes -o ConnectTimeout=15 "${REMOTE_USER}@${REMOTE_HOST}" \
  "cd ~/${REMOTE_DIR} && source .venv/bin/activate && python scripts/test_setup.py"
