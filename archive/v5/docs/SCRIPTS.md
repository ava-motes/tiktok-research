# Scripts reference

Every script in [`scripts/`](../scripts/) grouped by role. Use the **Production**
bucket for day-to-day work. Migration scripts are one-shot. Eval/research scripts
are optional and are **not** production gates.

Canonical orchestrator: [`scripts/enrich_pipeline.py`](../scripts/enrich_pipeline.py).
All processing runs on the server `comm-cme-p01` (see [`../README.md`](../README.md)).

---

## Production

| Script | Purpose |
|--------|---------|
| `enrich_pipeline.py` | **v5.0 orchestrator** (enrich → `tiktok_video_enriched`); `--pipeline content_creators` / `news` / `keyword` write the isolated collection tables |
| `test_setup.py` | Env / API / connectivity sanity check |
| `validate_handles.py` | Validate a handle group via Research API `user/info` → CSV (does not edit config) |
| `run_content_creators.py` | **Pipeline 1 daily command** (`--date YYYY-MM-DD --sample`). Full operator runbook: [`docs/COLLECTION_PIPELINES.md`](COLLECTION_PIPELINES.md) |
| `collect_content_creators.py` | Pipeline 1 collection + CSV only |
| `validate_content_creators.py` | Pipeline 1 run summary |
| `run_news.py` | **Pipeline 2 daily command** (`--date YYYY-MM-DD --sample`). Runbook: [`COLLECTION_PIPELINES.md`](COLLECTION_PIPELINES.md) |
| `collect_news.py` | Pipeline 2 collection + CSV only |
| `validate_news.py` | Pipeline 2 run summary |
| `run_keyword.py` | **Pipeline 3 daily command** (`--date YYYY-MM-DD --sample`). Do not run the full 263-term list until the sample is reviewed. Runbook: [`COLLECTION_PIPELINES.md`](COLLECTION_PIPELINES.md) |
| `collect_keyword.py` | Pipeline 3 collection + CSV only |
| `validate_keyword.py` | Pipeline 3 run summary |
| `validate_pipelines_static.py` | Local/static checks for all three pipelines (no API / media / BQ writes) |
| `pull_videos.py` | Collect videos → SQLite |
| `pull_user_info.py` | Collect user/profile info → SQLite |
| `pull_recent_videos.py` | Incremental recent-video pull |
| `transcription_worker.py` | Whisper transcription worker (temp media → delete) |
| `ocr_worker.py` | Google Vision OCR worker |
| `emoji_worker.py` | Emoji extraction worker |
| `run_production_validation.py` | Hard validation gates (uniqueness, PKs, consistency) |
| `export_research_dataset.py` | Research CSV/Parquet export from BigQuery |
| `fix_bq_consistency.py` | Repair BQ consistency issues |
| `ensure_monitoring_views.py` | Create/refresh BQ monitoring views |
| `daily_enrichment_summary.py` | Daily ops summary |

## Migration / one-shot (archive after confirmed applied)

| Script | Purpose |
|--------|---------|
| `migrate_bq_simplified_architecture.py` | One-time BQ schema migration (**do not** re-run the `create_time`←`posted_at` alias) |
| `validate_bq_simplified_architecture.py` | Validate the above migration |
| `rebuild_tiktok_video_enriched.py` | Rebuild enriched table (creates backup tables) |

## Eval / research (optional; not production gates)

| Script | Purpose |
|--------|---------|
| `validate_enrichment_6videos.py`, `validate_enrichment_with_emojis.py`, `collect_six_validation_videos.py`, `export_text_layers_eval_6videos.py` | Six-video qualitative validation set |
| `enrich_videos_with_ocr.py` | **Legacy/eval** EasyOCR path (writes SQLite `onscreen_text`; does **not** reach BQ) |
| `ocr_eval_batch.py`, `collect_ocr_eval_samples.py`, `ocr_signal_evaluation.py` | OCR evaluation tooling (EasyOCR) |
| `production_readiness_500.py` | Scale readiness measurement |
| `post_run_acceptance_review.py` | Acceptance summary after a readiness run |
| `enrichment_quality_v4_1_report.py` | Historical quality report |
| `enrichment_cost_dashboard.py`, `analyze_emoji_frequency.py`, `analyze_partial_rows.py` | Ad-hoc analysis |
| `retry_enrichment_partials.py`, `single_account_run.py`, `debug_api_video_payload.py` | Operational helpers |
| `validate_emoji_extract.py` | Emoji extraction unit/live check |
| `export_text_layers_sample.py`, `collect_ocr_eval_samples.py` | Sampling helpers |

## Legacy (superseded; kept for reference)

| Script | Note |
|--------|------|
| `run_all.py` | Legacy collect+classify+CSV orchestration; **use `enrich_pipeline.py`** |
| `transcribe_videos.py`, `download_audio.py`, `download_and_transcribe.py` | Legacy persistent-audio transcription; superseded by `transcription_worker.py` |
| `export_csv.py` | Legacy SQLite multimodal export; research export is `export_research_dataset.py` |
| `run_full_validation.py` | Legacy full validation (invokes the EasyOCR path); use `run_production_validation.py` |
| `classify_videos.py`, `classify_accounts.py` | Classification helpers (SQLite variants) |

> The pre-v5.0 CSV-only scripts at the repo root have been moved to
> [`../legacy/`](../legacy/).
