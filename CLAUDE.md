# Project conventions (for AI assistants)

This project collects TikTok Research API v2 data and enriches it into BigQuery.
Current pipeline version: **`enrichment-v5.0`**. Start with
[`README.md`](README.md) and [`docs/SCHEMA.md`](docs/SCHEMA.md).

## Golden rules

- **Server-only processing.** All TikTok collection and enrichment run on the
  Moody server `comm-cme-p01` (via SSH). The laptop is for editing code, SSH, and
  browsing BigQuery. Never run production collection/enrichment locally, and never
  download TikTok media to a laptop.
- **Secrets** live in the server `.env` and GCP IAM. Credentials are read from
  environment variables (never hardcode). Never commit `.env` or service-account
  JSON keys.
- **Auth:** TikTok Research API uses OAuth 2.0 client-credentials flow; use the
  `requests` library. Always handle pagination and rate limiting.

## Architecture (do not redesign without explicit request)

```
TikTok Research API → SQLite staging (server) → Whisper + Vision OCR + emoji
  → BigQuery tiktok_video_enriched (+ tiktok_pipeline_logs) → validation → export
```

- **Analytics source of truth:** `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`.
- **Canonical orchestrator:** `scripts/enrich_pipeline.py` (use `--production` for daily runs).
- **Schema source of truth:** `tiktok/enrichment/bigquery_loader.py`
  (`BQ_SCHEMAS`, `RESEARCH_COLUMNS`, `OPERATIONAL_COLUMNS`) + `docs/SCHEMA.md`.

## Column / naming conventions

- Follow the cross-layer naming map in [`docs/SCHEMA.md`](docs/SCHEMA.md)
  (e.g. API `favorites_count` → SQLite `save_count` → BQ `favorite_count`;
  BQ creator handle is `creator_username`).
- Production transcript = BQ `whisper_transcript`; production OCR = BQ `ocr_text`.
  Legacy `transcripts` / `videos.onscreen_text` / EasyOCR paths are not analytics.

## Legacy

Pre-v5.0 CSV-only scripts live in [`legacy/`](legacy/) and must not be used for
production. New work goes through `scripts/` + the `tiktok/` package.
