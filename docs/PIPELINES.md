# How to run P1 / P2 / P3

All commands run **on `comm-cme-p01`**. Use a lagged `--date` (Research API is often 24–48 hours behind). Quota resets at midnight UTC = 7:00 PM Chicago. Same `--date` without `--reset-checkpoints` is a safe resume.

Keep the existing server `.env` after a git pull. Do not copy it into the repo or onto a laptop.

| | P1 | P2 | P3 |
|--|----|----|-----|
| Folder | `p1_content_creators/` | `p2_news/` | `p3_keywords/` |
| Client ID | …861 | …443 | …993 |
| Env | `TIKTOK_CLIENT_*` | `NEWS_API_*` only | `KEYWORD_SEARCH_API_*` only |
| Input | 526 handles | 137 handles | 263 keywords (daily = 5-term sample) |
| BigQuery | `content_creators` | `news` | `keyword` |
| Daily | `run_content_creators.py` | `run_news.py` | `run_keyword.py --sample` |
| Local CSV | `p1_content_creators/results/csv/` | `p2_news/results/csv/` | `p3_keywords/results/csv/` |
| GCS archive | `gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv` | `gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv` | `gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv` |

## Canonical daily commands

`--sample` is a smoke test for P1/P2, not the daily job. P3 daily **is** `--sample` until the five terms are reviewed.

```bash
DATE=YYYY-MM-DD

python p1_content_creators/scripts/run_content_creators.py \
  --date "$DATE" --utc-day --skip-whisper --continue-on-failures --skip-user-info

python p2_news/scripts/run_news.py \
  --date "$DATE" --utc-day --skip-whisper

python p3_keywords/scripts/run_keyword.py \
  --date "$DATE" --sample --utc-day --skip-whisper
```

P1 and P2 daily: OCR + emoji, skip Whisper. GCS after a completed run (API handle failures allowed; enrich/BQ/CSV validation must succeed).
**Scheduled entrypoint (P1 then P2, no P3, no clock time yet):** `bash common/scripts/run_daily_p1_p2.sh`

```bash
bash common/scripts/run_daily_p1_p2.sh --preflight
bash common/scripts/run_daily_p1_p2.sh
```

If `DATE` is unset, the job uses America/Chicago today minus `RESEARCH_LAG_DAYS` (default 2). P2 is not started if P1 fails. `--reset-checkpoints` is never passed. To attach a clock time later, only update the timer/crontab via `common/server/install_p1_p2_schedule.sh` — do not change runner flags.

Do not run three OCR jobs at once. Prefer P1, then P2, then P3.

Each `run_*.py` collects with that pipeline’s credentials, then calls **one copy** of `common/scripts/enrich_pipeline.py` with **only that pipeline’s** `--pipeline`:

| Runner | `--pipeline` | BigQuery table |
|--------|--------------|----------------|
| `run_content_creators.py` | `content_creators` | `content_creators` |
| `run_news.py` | `news` | `news` |
| `run_keyword.py` | `keyword` | `keyword` |

Do not invoke `enrich_pipeline.py` with another pipeline’s id from a given runner. That is how credentials and tables stay isolated without duplicating Whisper / OCR / emoji code.

Count P1/P2 videos with `collection_status = 'ok'`. `api_failed` rows are handle stubs (`video_id` like `handle_fail:YYYY-MM-DD:handle`), not videos.

P3 `--sample` is `news, trump, tsa, ice, netanyahu` — not the first five file terms. Do not run the full 263-term list until that sample is reviewed.

`--utc-day` queries one UTC calendar day (`start_date == end_date`). Omit it for a Chicago civil day (two inclusive UTC dates, then hours outside Chicago are dropped).

## GCS run archive

**Automatic:** each completed `run_*.py` for P1/P2 (collect finished, enrich/BQ ok, dated CSV built and validated) uploads that pipeline’s date CSV via `common/scripts/upload_run_csv.py`. Object name = runner `--date`. Same date overwrites. Opt out: `--skip-gcs`. Handle API failures (`collection_status=api_failed`) are included in the dated CSV and do **not** block GCS. Incomplete runs (stop reason, enrich/BQ failure, CSV export/validation failure) are not archived. A dated CSV is written under each pipeline’s `results/csv/YYYY-MM-DD.csv` after BQ sync (and again as a final export before upload for P1/P2).

P3 still archives only when that runner’s success gates pass (including zero API failures if that gate remains).

**Manual** (backfill a date that already has a CSV):

```bash
python common/scripts/upload_run_csv.py \
  --pipeline content_creators \
  --date YYYY-MM-DD \
  --file p1_content_creators/results/csv/YYYY-MM-DD.csv
```

Use `--pipeline news` or `keyword` and the matching `p2_news/results/csv/` or `p3_keywords/results/csv/` path (or another non-empty CSV for that run). Server needs the existing enrichment GCP credentials; see [`SERVER.md`](SERVER.md).
