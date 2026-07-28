# TikTok Research + Enrichment Pipeline

Collects TikTok Research API v2 data and enriches it (Whisper transcription,
Google Cloud Vision OCR, emoji extraction) into a single BigQuery analytics
table for research.

**Pipeline version:** `enrichment-v5.0` (Git tag `v5.0`, core architecture frozen).

> **Golden rule:** all TikTok collection and enrichment run **only** on the
> Moody server `comm-cme-p01`. Your laptop is for editing code, SSH, and browsing
> BigQuery — never run production collection/enrichment locally.

---

## Architecture

```text
TikTok Research API
        │
        ▼
Collection (comm-cme-p01)  →  SQLite staging
        │
        ├─ Whisper transcription
        ├─ Google Vision OCR
        └─ Emoji extraction
        │
        ▼
BigQuery: tiktok_video_enriched  (+ tiktok_pipeline_logs)
        │
        ▼
Production validation  →  research CSV / Parquet export
```

- **Analytics source of truth:** BigQuery
  `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`.
- **Staging:** SQLite `data/tiktok_research.db` on the server.

---

## Documentation map

| Doc | Purpose |
|-----|---------|
| [`docs/SCHEMA.md`](docs/SCHEMA.md) | **Canonical** tables, columns, naming map, source-of-truth rules |
| [`docs/SCRIPTS.md`](docs/SCRIPTS.md) | Which scripts are production vs migration vs eval |
| [`docs/PIPELINE_ARCHITECTURE.md`](docs/PIPELINE_ARCHITECTURE.md) | System map + column meanings |
| [`docs/ENRICHMENT.md`](docs/ENRICHMENT.md) | Server + GCP enrichment setup |
| [`docs/AVA_ONBOARDING.md`](docs/AVA_ONBOARDING.md) | Step-by-step onboarding (VPN → server → BigQuery) |
| [`RELEASE.md`](RELEASE.md) | v5.0 milestone / release notes |
| [`CLAUDE.md`](CLAUDE.md) | Conventions for AI assistants working in this repo |

---

## Canonical commands (run on the server)

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"     # ffmpeg lives in ~/bin on this VM
```

| Task | Command |
|------|---------|
| Sanity check env/API | `python scripts/test_setup.py` |
| Ensure BQ schema | `python scripts/enrich_pipeline.py --ensure-bq-schema` |
| Inspect BQ schema | `python scripts/enrich_pipeline.py --inspect-bq-schema` |
| Small enrichment test | `python scripts/enrich_pipeline.py --group batch_test --limit 6 --sync-bigquery` |
| Incremental enrich + validate | `python scripts/enrich_pipeline.py --group batch_test --limit 50 --incremental --sync-bigquery --validate` |
| Production run | `python scripts/enrich_pipeline.py --production --group <group>` |
| Production validation | `python scripts/run_production_validation.py` |
| Fix BQ consistency | `python scripts/fix_bq_consistency.py` |
| Export research dataset | `python scripts/export_research_dataset.py` |

Handle groups (`sample`, `test`, `complete`, `batch_test`, ...) are defined in
[`config.yaml`](config.yaml). `enrich_pipeline.py` is the canonical orchestrator.

---

## Requirements

```bash
pip install -r requirements.txt              # core collection
pip install -r requirements-enrichment.txt   # Whisper / Vision / BigQuery / export
```

`requirements-ocr.txt` (EasyOCR/torch) is only for the optional research OCR eval
stack; it is not part of the production pipeline.

---

## Legacy code

Pre-v5.0 CSV-only scripts have been moved to [`legacy/`](legacy/) and are not part
of the production pipeline. See [`legacy/README.md`](legacy/README.md).

---

## Secrets

Credentials live only in the server `.env` and GCP IAM. Never commit `.env` or
service-account JSON keys. See [`.env.example`](.env.example) for the required
keys.
