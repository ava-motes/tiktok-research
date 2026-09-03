# Shared infrastructure

One copy of code used by P1, P2, and P3. Do not duplicate these packages into pipeline folders.

| Path | Role |
|------|------|
| `api/` | TikTok Research API client (`video/query`, `user/info`, download) |
| `tiktok/` | Auth, SQLite, pipeline registry, collection, checkpoints, Box, server guard |
| `enrichment/` | Whisper, Vision OCR, emoji, BigQuery upsert |
| `scripts/` | Shared workers + `enrich_pipeline.py --pipeline …` + `upload_run_csv.py` |
| `server/` | Deploy / venv / GCP env helpers for `comm-cme-p01` |
| `config.yaml` | Pipeline registry, sample handles, Box folder IDs, enrichment defaults |
| `bootstrap.py` | Puts `common/` on `sys.path` and chdirs to the repo root |

Credentials stay in the **server `.env`**. This folder has no API keys.

`enrich_pipeline.py` is shared infrastructure (one Whisper / OCR / emoji implementation). Each pipeline must invoke it with **only its own** `--pipeline` value:

- P1 `run_content_creators.py` → `--pipeline content_creators` → table `content_creators`
- P2 `run_news.py` → `--pipeline news` → table `news`
- P3 `run_keyword.py` → `--pipeline keyword` → table `keyword`

The command **requires** `--pipeline` and never writes `tiktok_video_enriched`. Do not call it from one pipeline with another pipeline’s id.
