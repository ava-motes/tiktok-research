# Architecture

Three isolated pipelines share SQLite staging and Whisper / Vision / emoji workers, then write **separate** BigQuery tables.

```text
TikTok Research API
        │
        ├── P1 content_creators  (client …861)
        ├── P2 news              (client …443)
        └── P3 keyword           (client …993)
                │
                ▼
        SQLite on comm-cme-p01
                │
                ▼
        Whisper + Vision OCR + emoji   (common/enrichment + common/scripts/*_worker.py)
                │
                ▼
        BigQuery: content_creators | news | keyword
                │
                ▼
        CSV in pipeline results/ + UT Box
```

`common/scripts/enrich_pipeline.py` is shared infrastructure: one copy of Whisper / Vision / emoji. Each runner must pass **only its own** `--pipeline` (`content_creators`, `news`, or `keyword`) so BigQuery writes stay on that pipeline’s table. It does not write `tiktok_video_enriched`. That old workflow is in `archive/v5/`.

Python imports: add `common/` to `sys.path` via `common/bootstrap.py`. Packages: `api`, `tiktok`, `enrichment`. `tiktok.api` / `tiktok.enrichment` still resolve for archived scripts.

Server guard: `tiktok.collection.server_guard.require_collection_server()` — refuse any host other than `comm-cme-p01`.
