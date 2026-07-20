#!/usr/bin/env bash
# Paste this ENTIRE script into your active SSH session on comm-cme-p01 (one time).
# It lets your Mac connect without a password so Cursor can deploy and run jobs.

set -euo pipefail

PUBKEY='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHIvlLKcbyfhE6DRZ6LzsnI62l4bZG0XBjolx0n6AOmN sivanaraharisetty@users.noreply.github.com'

mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

if grep -qF "$PUBKEY" ~/.ssh/authorized_keys 2>/dev/null; then
  echo "SSH key already authorized."
else
  echo "$PUBKEY" >> ~/.ssh/authorized_keys
  echo "SSH key added."
fi

echo "Done. From your Mac, test with:"
echo "  ssh cme-user1@comm-cme-p01.moody.utexas.edu hostname"
