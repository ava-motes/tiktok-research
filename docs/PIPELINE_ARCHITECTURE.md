# TikTok enrichment pipeline architecture

Production design for Moody / CFME media engagement research.  
**Do not process TikTok media on laptops** — enrichment runs on `comm-cme-p01`.

**Pipeline version: `enrichment-v5.0`** (frozen core; research features only after this).  
**Canonical release notes:** [`RELEASE.md`](../RELEASE.md) (Git tag `v5.0`).

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
              BigQuery upsert
         tiktok_video_enriched
                       │
                       ▼
           Production validation
                       │
                       ▼
           Research dataset export
              (CSV / Parquet)
                       │
                       ▼
           Pipeline logs / metrics
```

Operational events append to:

`cfme-mediaengagment-prod.tiktok_research.tiktok_pipeline_logs`

Legacy BQ tables (`videos_raw`, `video_transcripts`, `video_ocr`, `video_emojis`) are **not** written.

## Components

| Stage | Where | Role |
|-------|-------|------|
| Collection | `comm-cme-p01` | Research API pull → SQLite `videos` / `users` / `comments` |
| Whisper | server workers | Temp download → ffmpeg WAV → OpenAI/faster-whisper → delete audio |
| Vision OCR | server workers | Temp video → keyframes → Cloud Vision (retries) → delete video |
| Emoji | server workers | Extract from caption / OCR / transcript / stickers |
| BigQuery sync | `sync_video_from_sqlite` | `DELETE` by `video_id` + `INSERT` + dedupe guard |
| Validation | `run_production_validation.py` | Hard gates: uniqueness, PKs, Whisper/OCR consistency |
| Export | `export_research_dataset.py` | Researcher CSV/Parquet (research columns only) |

## BigQuery: `tiktok_video_enriched`

One analytics row per `video_id`. Research and operational fields share one table but are grouped logically.

### Research columns

Identifiers, creator, engagement, captions, Whisper transcript, primary OCR (`ocr_text`), emojis, comments.

| Column | Description |
|--------|-------------|
| `video_id` | TikTok video id (logical primary key) |
| `video_url` | Canonical watch URL |
| `creator_username` | Handle without `@` |
| `creator_display_name` | Profile display name |
| `creator_bio` | Profile bio |
| `creator_verified` | Verified badge |
| `creator_followers` | Follower count |
| `creator_following` | Following count |
| `creator_total_likes` | Lifetime likes on account |
| `creator_video_count` | Account video count |
| `posted_at` | Publish time (string; ideally UTC) |
| `caption` | Post caption / description |
| `hashtags` | Hashtag string from collection |
| `like_count` | Likes |
| `comment_count` | Comments |
| `share_count` | Shares |
| `favorite_count` | Saves / favorites |
| `video_duration_seconds` | Duration |
| `comments_json` | Optional JSON array of comment objects |
| `voice_to_text` | TikTok API ASR / voice-to-text when present |
| `sticker_text` | Sticker / overlay text from API metadata |
| `whisper_transcript` | ASR transcript from enrichment Whisper |
| `ocr_text` | **Primary research OCR** = cleaned text |
| `emoji_characters` | Pipe-joined emoji glyphs |
| `emoji_descriptions` | CLDR / research names |
| `emoji_category` | Semantic categories |

### Operational columns

Quality, latency, pipeline provenance, failure detail. Prefer `tiktok_pipeline_logs` for stage-level retries/errors.

| Column | Description |
|--------|-------------|
| `whisper_status` | `ok` / `failed` / `missing` (never `ok` with empty transcript) |
| `whisper_latency_seconds` | Worker latency |
| `raw_ocr_text` | Full Vision text before garbage filtering |
| `cleaned_ocr_text` | Deduped / filtered OCR |
| `ocr_quality_score` | 0–100 OCR cleanliness score |
| `ocr_character_count` | Length of cleaned OCR |
| `ocr_unique_text_ratio` | Unique-block ratio after dedupe |
| `ocr_source_count` | Distinct OCR source labels |
| `emoji_source` | Text layers where emojis were found |
| `enrichment_status` | `ok` / `partial` / `failed` |
| `enrichment_quality_score` | Modality coverage 0–100 |
| `failure_reason` | Active failure summary (empty when healthy) |
| `enrichment_date` | UTC date of last enrichment write (`YYYY-MM-DD`) |
| `pipeline_version` | `enrichment-v5.0` |

Constants `RESEARCH_COLUMNS` / `OPERATIONAL_COLUMNS` live in `tiktok/enrichment/bigquery_loader.py`.

## BigQuery: `tiktok_pipeline_logs`

Append-only ops log: stage, status, retry_count, timings, error_type/message, hostname.  
Includes `production_validation` stage events from the orchestrator.

## Operational safeguards (v5.0)

| Safeguard | Implementation |
|-----------|----------------|
| Idempotent upserts | `DELETE` + `INSERT` + post-sync dedupe per `video_id` |
| Incremental processing | `--incremental` / `--production` skips complete videos |
| Structured logging | `WorkerTimer` + `enrichment_log` → `tiktok_pipeline_logs` |
| Retry + backoff | `with_retries` for Whisper and Vision OCR |
| Temp media cleanup | `temporary_audio` / `temporary_video` (`finally` rmtree) |
| Monitoring | Metrics JSON, `daily_enrichment_summary.py`, monitoring SQL views |
| Validation gate | Runs after BQ sync; non-zero exit on hard failures |

## Quality rubric (summary)

Metadata 20 · Whisper 30 · OCR 30 · voice_to_text 10 · emoji 5 · sticker 5.  
Emoji is optional — speech-only videos are not treated as failures.

## Production commands (server)

```bash
ssh cme-p01
cd ~/tiktok_research && source .venv/bin/activate && set -a && source .env && set +a

# Daily production job (incremental + sync + validate + export)
python scripts/enrich_pipeline.py --production --group <group>

# One-shot consistency repair (dedupe + known bad rows + validate)
python scripts/fix_bq_consistency.py

# Validate only
python scripts/run_production_validation.py

# Research export only
python scripts/export_research_dataset.py

# Daily ops summary
python scripts/daily_enrichment_summary.py
```

## Production-ready checklist

- [x] One row per `video_id` (dedupe guard + validation)
- [x] Validation automated in `--production` flow
- [x] Idempotent BQ upserts
- [x] Incremental enrichment
- [x] Failed videos retryable without clobbering successes (`--force` / retry scripts)
- [x] Temp media always cleaned up
- [x] BigQuery is the analytics source of truth
- [x] Docs + monitoring summaries

After this freeze, prefer research features (screenshot OCR, comments, multilingual, analysis) over core pipeline changes.

## Related docs

- `docs/ENRICHMENT.md` — setup / credentials  
- `docs/AVA_ONBOARDING.md` — researcher / collaborator onboarding  
- `data/proposed_quality_rubric.md` — scoring design  
- `data/production_checklist.json` — go/no-go checklist  
