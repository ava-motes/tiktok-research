# TikTok research

Three isolated collection pipelines. Open a folder to see which one it is:

```text
p1_content_creators/   client …861   BigQuery content_creators   526 handles
p2_news/               client …443   BigQuery news               137 handles
p3_keywords/           client …993   BigQuery keyword            263 terms
common/                shared API, enrichment, Box, server
archive/               old v5.0 / legacy / discovery / eval
```

**Golden rule:** collection and enrichment run only on Moody server `comm-cme-p01`. The laptop is for editing, Git, SSH, and BigQuery. Never download TikTok media locally. Secrets live in the server `.env` — never commit them, and never copy that file onto a laptop.

Each pipeline has its own API credentials, input list, BigQuery table, results, logs, and Box folder. None of them write `tiktok_video_enriched`.

Shared enrichment lives in `common/scripts/enrich_pipeline.py`. Each runner calls it with **only that pipeline’s** `--pipeline` value (`content_creators`, `news`, or `keyword`) so credentials and BigQuery tables stay isolated without duplicating worker code.

## Canonical daily run (server)

Do not treat `--sample` as the daily job. Smoke tests stay in each pipeline README.

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

DATE=YYYY-MM-DD   # lagged research date
```

| Pipeline | Production command |
|----------|-------------------|
| P1 | `python p1_content_creators/scripts/run_content_creators.py --date "$DATE" --utc-day --skip-whisper --continue-on-failures --skip-user-info` |
| P2 | `python p2_news/scripts/run_news.py --date "$DATE" --utc-day --skip-whisper` |
| P3 | `python p3_keywords/scripts/run_keyword.py --date "$DATE" --sample --utc-day --skip-whisper` |

P3’s daily default is the five-term sample (`news, trump, tsa, ice, netanyahu`). Drop `--sample` only after that sample is reviewed — the full 263-term list can exhaust the keyword quota.

Sequential wrapper (same flags): `bash common/scripts/run_daily_all.sh`

Do **not** commit this layout until the reorganized paths have been tested on `comm-cme-p01`. After pulling to the server, keep the existing server `.env` in place.

Static check (laptop-safe, no API): `python common/scripts/validate_pipelines_static.py`

Docs: [`docs/PIPELINES.md`](docs/PIPELINES.md) · [`docs/SCHEMA.md`](docs/SCHEMA.md) · [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/SERVER.md`](docs/SERVER.md)
