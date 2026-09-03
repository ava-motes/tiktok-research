# Project conventions (for AI assistants)

Start with [`README.md`](README.md). Active work is **P1 / P2 / P3 only**. Do not revive `tiktok_video_enriched` unless explicitly asked.

## Golden rules

- **Server-only processing.** Collection and enrichment run on `comm-cme-p01`. Laptop: edit, Git, SSH, BigQuery.
- **Secrets** live in the server `.env` and GCP IAM. Never commit `.env` or service-account JSON.
- **Do not mix credentials.** P1 = `TIKTOK_CLIENT_*` / `CONTENT_CREATOR_TIKTOK_*` (client …861). P2 = `NEWS_API_*` only (…443). P3 = `KEYWORD_SEARCH_API_*` only (…993).
- **Do not mix BigQuery tables.** P1 → `content_creators`, P2 → `news`, P3 → `keyword`. Never write `tiktok_video_enriched` from an active pipeline.
- **Do not duplicate shared code.** API, SQLite, enrichment, Box, server guard live under `common/`.
- **Do not import `archive/`** from active pipelines.

## Layout

```text
p1_content_creators/   P1 scripts, 526-handle list, results, logs, Box copies
p2_news/               P2 scripts, 137-handle list, results, logs, Box copies
p3_keywords/           P3 scripts, 263-keyword list, results, logs, Box copies
common/                api/, tiktok/, enrichment/, scripts/, server/, config.yaml
archive/               v5/, legacy/, discovery/, evaluation/, deprecated/
docs/                  SCHEMA, ARCHITECTURE, SERVER, PIPELINES
```

Schema source of truth: `common/enrichment/bigquery_loader.py` + `docs/SCHEMA.md`.
Pipeline registry: `common/tiktok/pipelines.py` + `common/config.yaml`.
Canonical enrichment entry: `common/scripts/enrich_pipeline.py` with **that pipeline’s** `--pipeline content_creators|news|keyword` only. Do not duplicate the workers into pipeline folders.
