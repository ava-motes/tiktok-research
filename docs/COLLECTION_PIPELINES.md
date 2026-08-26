# Daily collection pipelines

Three **independent** collection paths. Implement and test **one at a time**.
All TikTok API work runs only on `comm-cme-p01` (never on a laptop).

| Pipeline id | Status | Source list | API | BigQuery table |
|-------------|--------|-------------|-----|----------------|
| `content_creators` | **Implemented (not yet sample-validated on server)** | `handle_groups.complete` (505); sample = `batch_test` | Existing `TIKTOK_CLIENT_*` (`CONTENT_CREATOR_API`) | `tiktok_content_creators` |
| `news_accounts` | **Not production** — list and `NEWS_API` not confirmed | — | — | not created |
| `keyword_search` | **Not production** — wait for Pipeline 1 (+ 2) | canonical keywords in `config/keywords/mediacloud_march_2026.txt` (263) | — | not created |

v5.0 production (`scripts/enrich_pipeline.py` without `--pipeline`, table `tiktok_video_enriched`) is **unchanged**.

## Daily date window

- `--date YYYY-MM-DD` is a civil date in **America/Chicago** (`research.timezone`).
- Collection interval is UTC `[start_of_day, start_of_next_day)`.
- The Research API is queried with a non-empty YYYYMMDD range that covers that window; videos are then filtered by `create_time` so adjacent UTC days are dropped.
- Storage timestamps stay UTC.
- `--days 1` on the old `date_chunks(start==end)` path is **not** used for Pipeline 1.

Example: `--date 2026-08-25` → 2026-08-25 00:00 America/Chicago through 2026-08-26 00:00 America/Chicago (exclusive end).

## Pipeline 1 — content creators

### Source list
- Production list: `handle_groups.complete` (**505** unique). Do not silently replace with the 527-handle spreadsheet.
- Sample: `handle_groups.batch_test` — `underthedesknews`, `aaronparnas1`
- Spreadsheet source (audit only): `Newsfluencer List (CURRENT) (2).xlsx` Combined List
- Dirty handle left in `complete` until reviewed: `taternewsnetwork (formerly @mashtaternews; changed username)` — skipped at collection (not guessed)

### What it collects
Username query via existing `tiktok/api/videos.py` (`query_videos_for_chunk`), plus `research/user/info` for profile fields.

API metadata includes existing v5 fields plus `view_count`, `region_code`, `video_mention_list`, `video_label`, `effect_ids`, `music_id` when the API returns them.

### Enrichment (shared, not duplicated)
Same production workers: Whisper (`voice_to_text` is never overwritten), Google Vision OCR, emoji. Temp media is deleted.

### Deduplication
- Identity: `video_id`
- SQLite upsert of API metadata (engagement can refresh; enrichment columns are not wiped)
- Checkpoints: `data/checkpoints/content_creators_{group}_{YYYY-MM-DD}.json`
- BigQuery: `DELETE` + `INSERT` on `tiktok_content_creators` by `video_id`

### Commands (server only)

```bash
ssh cme-p01
cd ~/tiktok_research
git pull
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

# First test — two handles, one research day. Do not use complete (505).
python scripts/run_content_creators.py --date 2026-08-25 --sample
```

Collection-only:

```bash
python scripts/collect_content_creators.py --date 2026-08-25 --sample
```

Outputs (gitignored under `data/`):

- `data/exports/content_creators/content_creators_videos_<ts>.csv`
- `data/exports/content_creators/content_creators_run_<ts>.json`
- `data/exports/content_creators/content_creators_validation_<ts>.json`

Do **not** dual-write these rows to `tiktok_video_enriched`.

### Credentials
`CONTENT_CREATOR_TIKTOK_CLIENT_KEY` / `SECRET` if set; otherwise existing `TIKTOK_CLIENT_KEY` / `TIKTOK_CLIENT_SECRET`. Do not invent keys.

## Pipeline 2 and 3

Placeholders only. `scripts/collect_news_accounts.py` and `scripts/collect_keyword_search.py` exit with a not-implemented message. Do not run them. Do not create `tiktok_news_accounts` or `tiktok_keyword_search` until those pipelines are approved.
