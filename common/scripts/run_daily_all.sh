#!/bin/bash
# Daily P1 + P2 + P3 on comm-cme-p01. Edit the SETTINGS block, then run.
#   bash common/scripts/run_daily_all.sh
# Or one line:
#   DATE=2026-08-28 OCR=1 EMOJI=1 WHISPER=0 P1=1 P2=1 P3=1 P3_SAMPLE=1 bash common/scripts/run_daily_all.sh
#
# P1/P2/P3 use separate TikTok apps (separate 1,000-request daily quotas).
# P3_SAMPLE=1 is the safe default (5 keywords). P3_SAMPLE=0 is the full 263-term
# list and can exhaust Pipeline 3 quota on a few broad terms.
set -u

############################
# SETTINGS (edit these)
############################
DATE="${DATE:-2026-08-28}"   # research date YYYY-MM-DD
UTC_DAY="${UTC_DAY:-1}"      # 1 = TikTok start_date == end_date (one UTC day)

P1="${P1:-1}"                # content creators
P2="${P2:-1}"                # news / institutional handles
P3="${P3:-1}"                # keyword search

P1_SAMPLE="${P1_SAMPLE:-0}"  # 1 = two handles only
P2_SAMPLE="${P2_SAMPLE:-0}"  # 1 = two handles only
P3_SAMPLE="${P3_SAMPLE:-1}"  # 1 = news,trump,tsa,ice,netanyahu (keep this 1)

OCR="${OCR:-1}"              # 1 = Google Vision OCR
EMOJI="${EMOJI:-1}"          # 1 = emoji extract
WHISPER="${WHISPER:-0}"      # 1 = Whisper transcripts (slow / OpenAI credits)

SKIP_BQ="${SKIP_BQ:-0}"      # 1 = enrich SQLite only, do not upsert BigQuery
SKIP_COLLECT="${SKIP_COLLECT:-0}"  # 1 = enrich already-collected rows for DATE
BACKGROUND="${BACKGROUND:-1}"      # 1 = nohup (hours-long); 0 = this terminal

############################
# internals
############################
ROOT="${ROOT:-$HOME/tiktok_research}"
cd "$ROOT"

if [[ "${_DAILY_INNER:-}" != "1" && "${BACKGROUND}" == "1" ]]; then
  WRAP_LOG_DIR="p1_content_creators/logs"
  [[ "${P1:-1}" != "1" && "${P2:-1}" == "1" ]] && WRAP_LOG_DIR="p2_news/logs"
  [[ "${P1:-1}" != "1" && "${P2:-1}" != "1" ]] && WRAP_LOG_DIR="p3_keywords/logs"
  mkdir -p "$WRAP_LOG_DIR"
  STAMP=$(date -u +%Y%m%d_%H%M%S)
  LOG="${WRAP_LOG_DIR}/run_daily_all_${DATE}_${STAMP}.log"
  export _DAILY_INNER=1
  export DATE UTC_DAY P1 P2 P3 P1_SAMPLE P2_SAMPLE P3_SAMPLE
  export OCR EMOJI WHISPER SKIP_BQ SKIP_COLLECT BACKGROUND ROOT
  nohup bash "$0" "$@" > "$LOG" 2>&1 &
  echo "started pid=$!  log=$ROOT/$LOG"
  echo "tail: tail -f $ROOT/$LOG"
  exit 0
fi

set -euo pipefail
source .venv/bin/activate
set -a
# shellcheck disable=SC1091
source .env
set +a
export PATH="$HOME/bin:$PATH"
export PYTHONUNBUFFERED=1
mkdir -p p1_content_creators/logs p2_news/logs p3_keywords/logs

flag_on() { [[ "${1:-0}" == "1" ]]; }

STEPS=""
flag_on "$WHISPER" && STEPS="${STEPS},transcript"
flag_on "$OCR" && STEPS="${STEPS},ocr"
flag_on "$EMOJI" && STEPS="${STEPS},emoji"
STEPS="${STEPS#,}"

has_flag() {
  local script="$1" needle="$2"
  python "$script" --help 2>/dev/null | grep -q -- "$needle"
}

collect_flags() {
  local script="$1"
  local flags=(--date "$DATE" --skip-enrich --skip-bigquery)
  flag_on "$SKIP_COLLECT" && flags+=(--skip-collect)
  if flag_on "$UTC_DAY" && has_flag "$script" --utc-day; then
    flags+=(--utc-day)
  fi
  printf '%s\n' "${flags[@]}"
}

newest_full_run() {
  local prefix="$1"
  python - <<PY
from pathlib import Path
dirs = {
    "content_creators": Path("p1_content_creators/results/summaries"),
    "news": Path("p2_news/results/summaries"),
    "keyword": Path("p3_keywords/results/summaries"),
}
p = dirs["${prefix}"]
files = sorted(p.glob("${prefix}_full_run_*.json"), key=lambda x: x.stat().st_mtime) if p.is_dir() else []
print(str(files[-1]) if files else "")
PY
}

ids_from_full_run() {
  local json_path="$1"
  python - <<PY
import json, sys
from pathlib import Path
p = Path("$json_path")
if not p.is_file():
    sys.exit(0)
d = json.loads(p.read_text())
ids = d.get("ids_paths") or []
if ids:
    print(ids[-1])
elif d.get("ids_path"):
    print(d["ids_path"])
PY
}

run_pipeline() {
  local name="$1" script="$2" pipeline_id="$3" export_prefix="$4"
  shift 4
  local extra=("$@")

  echo "======== $name collect $DATE ========"
  local before rc=0
  before=$(newest_full_run "$export_prefix")
  local flags
  mapfile -t flags < <(collect_flags "$script")
  python "$script" "${flags[@]}" "${extra[@]}" || rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "WARN: $name collect exit=$rc (will still enrich any ids this run wrote)"
  fi

  if [[ -z "$STEPS" ]]; then
    echo "$name: enrichment skipped (OCR=0 EMOJI=0 WHISPER=0)"
    return 0
  fi

  local after ids
  after=$(newest_full_run "$export_prefix")
  if [[ -z "$after" || "$after" == "$before" ]]; then
    echo "STOP: $name did not write a new ${export_prefix}_full_run_*.json"
    return 1
  fi
  ids=$(ids_from_full_run "$after")
  if [[ -z "$ids" || ! -s "$ids" ]]; then
    echo "$name: no video ids to enrich"
    return 0
  fi

  echo "======== $name enrich steps=$STEPS file=$ids ========"
  local enrich=(
    python common/scripts/enrich_pipeline.py
    --pipeline "$pipeline_id"
    --steps "$STEPS"
    --incremental
    --video-ids-file "$ids"
  )
  if ! flag_on "$SKIP_BQ"; then
    enrich+=(--sync-bigquery)
    enrich+=(--collection-date "$DATE")
  fi
  "${enrich[@]}"
}

echo "DATE=$DATE UTC_DAY=$UTC_DAY P1=$P1 P2=$P2 P3=$P3"
echo "P3_SAMPLE=$P3_SAMPLE OCR=$OCR EMOJI=$EMOJI WHISPER=$WHISPER SKIP_BQ=$SKIP_BQ"
echo "steps=${STEPS:-none}"

if flag_on "$P3" && ! flag_on "$P3_SAMPLE"; then
  echo "WARN: P3_SAMPLE=0 runs the full 263-keyword list and can exhaust KEYWORD_SEARCH quota."
fi

OVERALL=0

if flag_on "$P1"; then
  extra=()
  flag_on "$P1_SAMPLE" && extra+=(--sample)
  if ! flag_on "$P1_SAMPLE" && has_flag p1_content_creators/scripts/run_content_creators.py --continue-on-failures; then
    extra+=(--continue-on-failures)
  fi
  run_pipeline "P1" p1_content_creators/scripts/run_content_creators.py content_creators content_creators "${extra[@]}" || OVERALL=1
fi

if flag_on "$P2"; then
  extra=()
  flag_on "$P2_SAMPLE" && extra+=(--sample)
  run_pipeline "P2" p2_news/scripts/run_news.py news news "${extra[@]}" || OVERALL=1
fi

if flag_on "$P3"; then
  extra=()
  flag_on "$P3_SAMPLE" && extra+=(--sample)
  run_pipeline "P3" p3_keywords/scripts/run_keyword.py keyword keyword "${extra[@]}" || OVERALL=1
fi

echo "======== done overall_exit=$OVERALL ========"
exit "$OVERALL"
