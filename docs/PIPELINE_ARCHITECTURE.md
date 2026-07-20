# TikTok enrichment pipeline architecture

Production design for Moody / CFME media engagement research.  
**Do not process TikTok media on laptops** — enrichment runs on `comm-cme-p01`.

## Flow

```text
TikTok Research API
        │
        ▼
Collection (comm-cme-p01)
        │
        ▼
SQLite staging (temporary)
        │
        ├──────────────┬──────────────┐
        ▼              ▼              ▼
    Whisper      Google Vision     Emoji
   (ffmpeg→WAV)   OCR (frames)    extraction
        │              │              │
        └──────────────┴──────────────┘
                       │
                       ▼
              BigQuery analytics
         tiktok_video_enriched
                       │
                       ▼
           Research dataset export
              (CSV / Parquet)
```

Operational events append to:

`cfme-mediaengagment-prod.tiktok_research.tiktok_pipeline_logs`

Legacy BQ tables (`videos_raw`, `video_transcripts`, `video_ocr`, `video_emojis`) are **not** written.

## Components

| Stage | Where | Role |
|-------|-------|------|
| Collection | `comm-cme-p01` | Research API pull → SQLite `videos` / `users` / `comments` |
| Whisper | server workers | Temp download → ffmpeg WAV → OpenAI/faster-whisper → delete audio |
| Vision OCR | server workers | Temp video → keyframes → Cloud Vision → delete video |
| Emoji | server workers | Extract from caption / OCR / transcript / stickers |
| BigQuery sync | `sync_video_from_sqlite` | `DELETE` by `video_id` + `INSERT` (no duplicates) |
| Export | `scripts/export_research_dataset.py` | Researcher CSV/Parquet without ops fields |

## BigQuery: `tiktok_video_enriched`

One analytics row per `video_id`.

### Identifiers

| Column | Description |
|--------|-------------|
| `video_id` | TikTok video id (logical primary key) |
| `video_url` | Canonical watch URL |

### Creator

| Column | Description |
|--------|-------------|
| `creator_username` | Handle without `@` |
| `creator_display_name` | Profile display name |
| `creator_bio` | Profile bio |
| `creator_verified` | Verified badge |
| `creator_followers` | Follower count |
| `creator_following` | Following count |
| `creator_total_likes` | Lifetime likes on account |
| `creator_video_count` | Account video count |

### Video metadata & engagement

| Column | Description |
|--------|-------------|
| `posted_at` | Publish time (string; ideally UTC) |
| `caption` | Post caption / description |
| `hashtags` | Hashtag string from collection |
| `like_count` | Likes |
| `comment_count` | Comments |
| `share_count` | Shares |
| `favorite_count` | Saves / favorites |
| `video_duration_seconds` | Duration |
| `comments_json` | Optional JSON array of comment objects |

### Native TikTok text

| Column | Description |
|--------|-------------|
| `voice_to_text` | TikTok API ASR / voice-to-text when present |
| `sticker_text` | Sticker / overlay text from API metadata |

### Whisper

| Column | Description |
|--------|-------------|
| `whisper_transcript` | ASR transcript from enrichment Whisper |
| `whisper_status` | `ok` / `error` / `missing` |
| `whisper_latency_seconds` | Worker latency (ops; omit from research CSV) |

### OCR

| Column | Description |
|--------|-------------|
| `ocr_text` | **Primary research OCR** = cleaned text |
| `raw_ocr_text` | Full Vision text before garbage filtering |
| `cleaned_ocr_text` | Deduped / filtered OCR (same intent as `ocr_text`) |
| `ocr_quality_score` | 0–100 OCR cleanliness score |
| `ocr_character_count` | Length of cleaned OCR |
| `ocr_unique_text_ratio` | Unique-block ratio after dedupe |
| `ocr_source_count` | Distinct OCR source labels |

### Emoji

| Column | Description |
|--------|-------------|
| `emoji_characters` | Pipe-joined emoji glyphs |
| `emoji_descriptions` | CLDR / research names (e.g. `ice cube`) |
| `emoji_category` | Semantic categories (emotion, object, …) |
| `emoji_source` | Text layers where emojis were found |

### Row status

| Column | Description |
|--------|-------------|
| `enrichment_status` | `ok` / `partial` / `failed` |
| `enrichment_quality_score` | Modality coverage 0–100 (see `data/proposed_quality_rubric.md`) |
| `failure_reason` | Active failure summary (empty when healthy) |
| `enrichment_date` | UTC date of last enrichment write (`YYYY-MM-DD`) |
| `pipeline_version` | e.g. `enrichment-v4.1` |

## BigQuery: `tiktok_pipeline_logs`

Append-only ops log: stage, status, retry_count, timings, error_type/message, hostname.

## Quality rubric (summary)

Metadata 20 · Whisper 30 · OCR 30 · voice_to_text 10 · emoji 5 · sticker 5.  
Emoji is optional — speech-only videos are not treated as failures.

## Production commands (server)

```bash
ssh cme-p01
cd ~/tiktok_research && source .venv/bin/activate && set -a && source .env && set +a

# Enrich + sync
python scripts/enrich_pipeline.py --group batch_test --limit 50 --incremental --sync-bigquery

# Validate
python scripts/run_production_validation.py

# Research export
python scripts/export_research_dataset.py
```

## Related docs

- `docs/ENRICHMENT.md` — setup / credentials  
- `data/proposed_quality_rubric.md` — scoring design  
- `data/final_partial_audit.json` — remaining partial classification  
- `data/production_checklist.json` — go/no-go checklist  
