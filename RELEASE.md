# Release notes

**Active production:** P1 content creators, P2 news, P3 keywords. Analytics tables are `content_creators`, `news`, and `keyword` — not `tiktok_video_enriched`.

Layout: `p1_content_creators/` · `p2_news/` · `p3_keywords/` · `common/` · `archive/`.

Daily runs (server `comm-cme-p01` only): see [`README.md`](README.md) and [`docs/PIPELINES.md`](docs/PIPELINES.md). Shared enrichment: `common/scripts/enrich_pipeline.py` with that pipeline’s `--pipeline` only.

---

## Historical: enrichment v5.0 (`tiktok_video_enriched`)

**Git tag:** `v5.0`  
**Status:** Archived under [`archive/v5/`](archive/v5/). Do not use for new collection.

That milestone froze Whisper + Vision OCR + emoji into a single BigQuery table `tiktok_video_enriched`. Column constants still live in `common/enrichment/bigquery_loader.py` (`RESEARCH_COLUMNS` / `OPERATIONAL_COLUMNS`); the table itself is not an active destination.

Frozen docs and scripts:

- `archive/v5/docs/PIPELINE_ARCHITECTURE.md`
- `archive/v5/docs/AVA_ONBOARDING.md`
- `archive/v5/scripts/run_production_validation.py`
- `archive/v5/scripts/export_research_dataset.py`
- `archive/v5/scripts/pull_videos.py`

**Do not process TikTok media on laptops.** Collection and enrichment still run only on `comm-cme-p01`.
