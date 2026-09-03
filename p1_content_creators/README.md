# P1 — Content Creators

TikTok **client ID ending 861**. Dedicated keys: `TIKTOK_CLIENT_*` or `CONTENT_CREATOR_TIKTOK_*`. Never use `NEWS_API_*` or `KEYWORD_SEARCH_API_*`.

| | |
|--|--|
| BigQuery table | `content_creators` |
| Input | 526 creator handles |
| Handle list | `config/newsfluencer_combined.txt` |
| Results | `results/csv/` · `results/parquet/` · `results/summaries/` |
| Logs / checkpoints | `logs/` · `logs/checkpoints/` |
| Local Box copies | `box/` (`YYYY-MM-DD.csv`) |
| GCS archive | `gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv` |
| Copy-paste SQL | `sql/content_creators.sql` |

Does **not** write `news`, `keyword`, or `tiktok_video_enriched`.

`run_content_creators.py` enriches by calling `common/scripts/enrich_pipeline.py --pipeline content_creators` only. After a fully successful run it archives the date CSV via `common/scripts/upload_run_csv.py` (same date overwrites).

Collection and enrichment run **only** on `comm-cme-p01`.

## Daily run (server)

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

DATE=YYYY-MM-DD   # lagged research date

# Canonical daily: 526 handles, OCR + emoji, skip Whisper
python p1_content_creators/scripts/run_content_creators.py \
  --date "$DATE" --utc-day --skip-whisper --continue-on-failures --skip-user-info

# Smoke test (2 handles) — not the daily job
python p1_content_creators/scripts/run_content_creators.py \
  --date "$DATE" --sample --utc-day --skip-whisper
```

## Validation

```bash
python p1_content_creators/scripts/validate_content_creators.py --date "$DATE"
python common/scripts/validate_pipelines_static.py   # all three pipelines, no API
```

Collect-only: `python p1_content_creators/scripts/collect_content_creators.py --date "$DATE"`

Failed-handle stubs: `python p1_content_creators/scripts/backfill_handle_api_failures.py --date "$DATE"`
