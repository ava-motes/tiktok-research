# P2 — News

TikTok **client ID ending 443**. Dedicated keys: `NEWS_API_CLIENT_KEY` / `NEWS_API_CLIENT_SECRET` only. No fallback to P1 or P3.

| | |
|--|--|
| BigQuery table | `news` |
| Input | 137 institutional news handles |
| Handle list | `config/news_accounts.txt` |
| Results | `results/csv/` · `results/parquet/` · `results/summaries/` |
| Logs / checkpoints | `logs/` · `logs/checkpoints/` |
| Local Box copies | `box/` (`YYYY-MM-DD.csv`) |
| GCS archive | `gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv` |
| Copy-paste SQL | `sql/news.sql` |

Does **not** write `content_creators`, `keyword`, or `tiktok_video_enriched`.

`run_news.py` enriches by calling `common/scripts/enrich_pipeline.py --pipeline news` only. After a fully successful run it archives the date CSV via `common/scripts/upload_run_csv.py` (same date overwrites).

Collection and enrichment run **only** on `comm-cme-p01`.

## Daily run (server)

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

DATE=YYYY-MM-DD

# Canonical daily: 137 handles, OCR + emoji, skip Whisper
python p2_news/scripts/run_news.py --date "$DATE" --utc-day --skip-whisper

# Smoke test (first two handles) — not the daily job
python p2_news/scripts/run_news.py --date "$DATE" --sample --utc-day --skip-whisper
```

## Validation

```bash
python p2_news/scripts/validate_news.py --date "$DATE"
python common/scripts/validate_pipelines_static.py
```

Collect-only: `python p2_news/scripts/collect_news.py --date "$DATE"`
