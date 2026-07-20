# TikTok Enrichment Pipeline v5.0

**Git tag:** `v5.0`  
**Pipeline version stamp:** `enrichment-v5.0`  
**Status:** Production-ready — **core architecture frozen**

Canonical milestone reference for collaborators. Detailed runbooks: `docs/PIPELINE_ARCHITECTURE.md`, `docs/AVA_ONBOARDING.md`.

---

## Highlights

What v5.0 delivers:

* Production-ready TikTok enrichment pipeline
* End-to-end automated pipeline (collect → enrich → BigQuery → validate → export)
* Google Cloud Vision OCR integration
* Whisper transcription
* Emoji extraction and normalization (CLDR descriptions)
* Single production BigQuery analytics table (`tiktok_video_enriched`)
* Operational logging (`tiktok_pipeline_logs`)
* Automatic validation before production completion
* Incremental enrichment with idempotent upserts
* Production exports (CSV and Parquet)
* Quality scoring and monitoring
* Research-ready dataset

---

## Architecture

```text
TikTok Research API
        │
        ▼
Collection (comm-cme-p01)
        │
        ▼
SQLite staging
        │
        ▼
Enrichment
  ├─ Whisper
  ├─ Google Vision OCR
  └─ Emoji extraction
        │
        ▼
BigQuery
  ├─ tiktok_video_enriched
  └─ tiktok_pipeline_logs
        │
        ▼
Production validation → Research export (CSV / Parquet)
```

**Do not process TikTok media on laptops.** All collection and enrichment runs on `comm-cme-p01`.

| Stage | Role |
|-------|------|
| Collection | Research API → SQLite (`videos` / `users` / comments staging) |
| Whisper | Temp download → ffmpeg WAV → transcription → delete audio |
| Vision OCR | Temp video → keyframes → Cloud Vision (retries) → delete video |
| Emoji | Extract from caption / OCR / transcript / stickers + CLDR descriptions |
| BigQuery | `DELETE` by `video_id` + `INSERT` + dedupe guard |
| Validation | Uniqueness, PKs, Whisper/OCR consistency gates |
| Export | Research columns only |

---

## Current status

* Production validated (hard checks PASS)
* ~516 enriched videos in BigQuery
* Whisper coverage: >99%
* OCR coverage: >97%
* Automated production validation in `--production` mode
* Ready for daily collection

---

## Overview

v5.0 turns TikTok Research API collection into a complete enrichment pipeline:

1. Collect videos on `comm-cme-p01` into SQLite staging  
2. Enrich with Whisper, Google Cloud Vision OCR, and emoji extraction  
3. Upsert one analytics row per `video_id` into BigQuery  
4. Run automated production validation  
5. Export a researcher-facing CSV/Parquet dataset  
6. Append operational events to pipeline logs  

---

## Key features (v5.0)

### Collection
- TikTok Research API v2 on `comm-cme-p01`
- Incremental collection into SQLite staging

### Enrichment
- Whisper transcription (OpenAI / faster-whisper backends)
- Google Cloud Vision OCR with early-frame sampling and postprocessing
- Emoji extraction with CLDR descriptions, categories, and source layers
- Modality quality scoring (metadata / Whisper / OCR / VTT / emoji / sticker)

### Storage
- **Analytics:** `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
- **Ops:** `cfme-mediaengagment-prod.tiktok_research.tiktok_pipeline_logs`
- Legacy BQ tables are **not** written

### Reliability
- Idempotent upserts (safe to rerun)
- Incremental enrichment (`--incremental` / `--production`)
- Exponential backoff retries (Whisper + Vision)
- Temporary media cleanup (`finally` paths)
- Automated validation with non-zero exit on hard failures
- Research dataset export wired into production mode

---

## BigQuery schema (logical groups)

Research and operational fields share `tiktok_video_enriched` but are grouped for clarity.  
Constants: `RESEARCH_COLUMNS` / `OPERATIONAL_COLUMNS` in `tiktok/enrichment/bigquery_loader.py`.

### Research columns
`video_id`, `video_url`, creator fields, `posted_at`, `caption`, `hashtags`, engagement counts, `video_duration_seconds`, `comments_json`, `voice_to_text`, `sticker_text`, `whisper_transcript`, `ocr_text`, emoji character / description / category fields.

### Operational columns
`whisper_status`, `whisper_latency_seconds`, `raw_ocr_text`, `cleaned_ocr_text`, OCR quality metrics, `emoji_source`, `enrichment_status`, `enrichment_quality_score`, `failure_reason`, `enrichment_date`, `pipeline_version`.

### Pipeline logs
Append-only: stage, status, retry_count, timings, error_type / message, hostname, including `production_validation` events.

Full column tables: `docs/PIPELINE_ARCHITECTURE.md`.

---

## Production requirements

| Requirement | Detail |
|-------------|--------|
| Host | `comm-cme-p01` (`cme-user1`) |
| Runtime | Python venv + `ffmpeg` on `PATH` (often `~/bin`) |
| Secrets | `.env` on server only — never commit |
| GCP | Project `cfme-mediaengagment-prod`, Vision SA key, BigQuery access |
| Source of truth | BigQuery analytics table (SQLite is staging only) |

---

## How to run

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

# Daily production job
python scripts/enrich_pipeline.py --production --group <group>

# Validate only
python scripts/run_production_validation.py

# Research export
python scripts/export_research_dataset.py

# Consistency repair (dedupe + known hygiene targets)
python scripts/fix_bq_consistency.py

# Ops summary
python scripts/daily_enrichment_summary.py
```

Production mode runs: incremental enrich → BQ upsert → validation → research export → metrics/logs.

---

## Known limitations

- OCR samples a capped set of frames (~12); short-lived overlays can still be missed on very long videos (mitigated by early 1s/3s/8s samples).
- Some videos have no speech → empty Whisper is expected; status must be `failed` / non-`ok`, not a false success.
- Emoji rate is content-dependent (often low); missing emoji is not a pipeline failure.
- Comment collection is optional / partial (`comments_json`); not a first-class enrichment stage yet.
- Large scale (>~100k videos/day) is not load-tested; stay on pilot/daily/weekly volumes first.
- Server Python is 3.9 (Google client libs emit EOL warnings); upgrade is operational, not architectural.

---

## Architecture freeze & roadmap

**Do not change the core v5.0 architecture** unless a bug is discovered or there is a compelling production reason.  
Priority from here: **collect and enrich research data**, not redesign the pipeline.

| Version | Focus |
|---------|--------|
| **v5.0** | ✅ Stable production pipeline |
| **v5.1** | Daily automated collection (cron / Cloud Scheduler) |
| **v5.2** | Comment collection |
| **v5.3** | Large-scale enrichment (10k+ videos) |
| **v6.0** | Research dashboard and analytics |

---

## Related docs

| Doc | Purpose |
|-----|---------|
| `docs/PIPELINE_ARCHITECTURE.md` | Schema + safeguards detail |
| `docs/AVA_ONBOARDING.md` | Collaborator onboarding / day-to-day commands |
| `docs/ENRICHMENT.md` | Credentials and enrichment setup |
| `docs/ENRICHMENT_CHANGELOG.md` | Earlier enrichment iteration history |
| `sql/monitoring_dashboard.sql` | Monitoring view definitions |

---

## Release checklist (v5.0)

- [x] One row per `video_id`
- [x] Validation passes with no hard errors
- [x] Idempotent daily reruns
- [x] Incremental processing
- [x] Failed videos retryable without clobbering successes
- [x] Temporary media cleaned up
- [x] BigQuery is the analytics source of truth
- [x] Documentation complete for this milestone
- [x] Monitoring / logs generated automatically
