#!/bin/bash
# Copy P1/P2 systemd units onto comm-cme-p01. Does NOT enable a schedule
# unless you pass --enable AND ON_CALENDAR.
#
#   bash common/server/install_p1_p2_schedule.sh
#   ON_CALENDAR='*-*-* 19:30:00' bash common/server/install_p1_p2_schedule.sh --enable
#
# ON_CALENDAR is systemd OnCalendar in the server's local timezone.
# Do not guess a time. The pipeline command is not changed by enabling.
set -euo pipefail

HOST="$(hostname | tr '[:upper:]' '[:lower:]')"
if [[ "$HOST" != *cme-p01* ]]; then
  echo "Refusing to install the collection schedule on host $(hostname)." >&2
  echo "This installer is for comm-cme-p01 only." >&2
  exit 1
fi

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$UNIT_DIR"

cp "$ROOT/common/server/tiktok-p1-p2.service" "$UNIT_DIR/tiktok-p1-p2.service"

TIMER_SRC="$ROOT/common/server/tiktok-p1-p2.timer"
TIMER_DST="$UNIT_DIR/tiktok-p1-p2.timer"
ENABLE=0
if [[ "${1:-}" == "--enable" ]]; then
  ENABLE=1
fi

if [[ "$ENABLE" == "1" ]]; then
  if [[ -z "${ON_CALENDAR:-}" ]]; then
    echo "Refusing to enable: set ON_CALENDAR to a systemd calendar expression." >&2
    echo "Example: ON_CALENDAR='*-*-* 19:30:00' bash common/server/install_p1_p2_schedule.sh --enable" >&2
    exit 1
  fi
  python3 - "$TIMER_SRC" "$TIMER_DST" "$ON_CALENDAR" <<'PY'
import sys
src, dst, calendar = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(src, encoding="utf-8").read()
lines = []
inserted = False
for line in text.splitlines():
    lines.append(line)
    if line.strip() == "[Timer]" and not inserted:
        lines.append(f"OnCalendar={calendar}")
        inserted = True
open(dst, "w", encoding="utf-8").write("\n".join(lines) + "\n")
PY
  systemctl --user daemon-reload
  systemctl --user enable --now tiktok-p1-p2.timer
  systemctl --user list-timers tiktok-p1-p2.timer --no-pager
  echo "Timer enabled with ON_CALENDAR=$ON_CALENDAR"
else
  systemctl --user daemon-reload
  systemctl --user disable --now tiktok-p1-p2.timer >/dev/null 2>&1 || true
  echo "Installed $UNIT_DIR/tiktok-p1-p2.service"
  echo "Timer is NOT installed or enabled. No run time is configured."
  echo "Later: ON_CALENDAR='*-*-* HH:MM:00' bash common/server/install_p1_p2_schedule.sh --enable"
fi
