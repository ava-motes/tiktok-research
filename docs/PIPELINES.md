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
| Box folder | `p1_content_creators` | `p2_news` | `p3_key_words` |

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

Sequential wrapper with the same defaults (`UTC_DAY=1`, `WHISPER=0`, `P3_SAMPLE=1`):

```bash
bash common/scripts/run_daily_all.sh
```

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
