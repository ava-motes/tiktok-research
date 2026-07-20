# Enrichment implementation change log

## 2026-07-17 — Simplified BigQuery architecture (v3)

### Production tables (only two)
- `tiktok_video_enriched` — analytics (one row per video)
- `tiktok_pipeline_logs` — ops/debug events

### Deprecated (do not write; drop after migration validation)
- `videos_raw`, `video_transcripts`, `video_ocr`, `video_emojis`

### Scripts
- `scripts/migrate_bq_simplified_architecture.py`
- `scripts/validate_bq_simplified_architecture.py`
- `sql/drop_legacy_bq_tables.sql`

SQLite staging names are unchanged and remain local-only.

## 2026-07-16e — Ops freeze: validation & monitoring (no new enrichment features)

### Focus
- Stop adding enrichment capabilities; prove stability via 500-video gate
- Acceptance criteria: collection >99%, OCR/Whisper ≥98%, BQ 100%, 0 dups/crashes

### Ops tooling
- `sql/monitoring_dashboard.sql` + `scripts/ensure_monitoring_views.py`
- `scripts/daily_enrichment_summary.py` (JSON + Markdown)
- `scripts/post_run_acceptance_review.py` (criteria + BQ sample rows)
- `tiktok/enrichment/validate_row.py` (pre-BQ required-field checks)
- `tiktok/enrichment/retry.py` (transient API backoff for Whisper)
- `enrich_pipeline.py --incremental` for safe daily reruns

## 2026-07-16d — Quality hardening (pipeline v2.3)

### Priority fixes before production
- **Whisper:** ffmpeg convert to WAV/MP3 before API; retry once on
  `format_not_supported`; log original/converted format; persist
  `duration_seconds` + fix `whisper_cost_estimate`
- **OCR:** Vision block/paragraph rebuild; adjacent-frame Jaccard dedupe;
  `video_ocr_stats` (`number_of_frames_processed`, `frames_with_text`,
  `average_text_per_frame`, confidence, language)
- **Emoji:** codepoint + kind (emoji/flag/modifier) + semantic category;
  drop non-semantic UI symbols (☑/⚫/®/…)
- **Quality:** `enrichment_quality_score` (100/90/80/60/40)
- **Timestamps:** `enrichment_pipeline_status` stage times → BQ columns
- **Cost:** `total_cost_estimate` + `scripts/enrichment_cost_dashboard.py`
- **Readiness:** `scripts/production_readiness_500.py` (500-video gate)

## 2026-07-16c — OCR post-process + emoji mapping fields

### Added
- OCR post-processing (`ocr_postprocess.py`): `raw_ocr_text`, `cleaned_ocr_text`,
  frame dedupe, `ocr_text_segments` JSON (text, frame_position, source_type, confidence)
- BQ fields: `emoji_characters`, `emoji_descriptions`, `emoji_count`, `emoji_sources`
- `scripts/validate_enrichment_with_emojis.py` (six URLs + ice-cube example)
- `scripts/analyze_emoji_frequency.py` → `data/emoji_frequency_report.json`
- CLDR fallbacks for coded-language emojis (🧊 ice cube, 🔫 water pistol, 🍉)

## 2026-07-16b — Production hardening before scale

### Added
- Monitoring fields on `tiktok_video_enriched`: `enrichment_status`,
  `failure_reason`, `pipeline_version`, `collection_date`, `enrichment_date`,
  `ocr_latency_seconds`, `whisper_latency_seconds`,
  `vision_api_cost_estimate`, `whisper_cost_estimate`
- Content OCR source labels (JSON): `video_overlay`, `twitter_screenshot`,
  `truth_social_screenshot`, `green_screen`, `news_screenshot`, etc.
  (`tiktok/enrichment/ocr_sources.py`)
- `scripts/validate_emoji_extract.py` — unit + live caption emoji → CLDR
- `enrich_pipeline.py --inspect-bq-schema` and sample metrics JSON

## 2026-07-16 — Single BigQuery table `tiktok_video_enriched`

### Changed
- BigQuery output is **one row per video** in
  `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
- Stopped creating/writing BQ tables `video_transcripts`, `video_ocr`,
  `video_emojis` (SQLite staging on `comm-cme-p01` unchanged)
- `sync_video_from_sqlite` upserts aggregated metadata + Whisper + OCR + emoji
- `validate_enrichment_6videos.py` upserts into the final table by default

### Fields
- Metadata: `video_id`, `creator_handle`, `video_url`, `description`,
  `hashtags`, `views`, `likes`, `comments`, `shares`, `posted_at`
- Whisper: `transcript`, `transcript_language`, `whisper_model`, `audio_available`
- OCR: `ocr_text`, `ocr_frames_processed`, `ocr_confidence_avg`, `ocr_sources`
- Emoji: `emojis`, `emoji_names`, `emoji_categories`

## 2026-07-13b — Server-only + cfme-mediaengagment-prod lock-in

### Changed
- Architecture docs: TikTok runs **only** on `comm-cme-p01`; Mac = SSH/dev only
- GCP hard-defaults: project `cfme-mediaengagment-prod`, dataset `tiktok_research`
- `.env.example`: `GCP_PROJECT`, `VISION_ENABLED`, server key path, Whisper=`openai`/`base`
- OCR sampling: **0 / 25 / 50 / 75 / 100%** (+ fill to max 8–12 frames)
- `bigquery_loader.py`: `gcp_project()` reads `BIGQUERY_PROJECT` or `GCP_PROJECT`
- `ocr_google.py`: respects `VISION_ENABLED`; clearer server credential errors

### Unchanged
- Collector / TikTok API auth paths
- Temp-media delete-after-use policy

## 2026-07-13 — Additive enrichment pipeline (no collector changes)

### Added
- `tiktok/enrichment/` package: temp media, Whisper backends, Google Vision OCR,
  emoji CLDR names, SQLite staging, BigQuery loader, worker logging
- Workers: `scripts/transcription_worker.py`, `ocr_worker.py`, `emoji_worker.py`
- Orchestrator: `scripts/enrich_pipeline.py`
- Validation: `scripts/validate_enrichment_6videos.py` (six research URLs)
- `requirements-enrichment.txt`, `docs/ENRICHMENT.md`
- `.env.example` enrichment keys; `config.yaml` `enrichment:` section

### Modified
- `tiktok/db.py` — `get_connection()` also creates enrichment staging tables
  (additive; safe for existing DBs)

### Not modified
- TikTok API collection scripts (`pull_videos.py`, `pull_user_info.py`, auth, client)
- Existing `download_and_transcribe.py` / EasyOCR path remain available

### Behavior notes
- Media is never retained: temp dirs deleted after each video
- Google Vision is primary OCR (EasyOCR not used by these workers)
- Whisper prefers `faster-whisper` on CPU; falls back to OpenAI Whisper API
- Failures are per-video; batch continues
