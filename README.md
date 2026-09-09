# TikTok research

Three isolated collection pipelines. Open a folder to see which one it is:

```text
p1_content_creators/   client …861   BigQuery content_creators   526 handles
p2_news/               client …443   BigQuery news               137 handles
p3_keywords/           client …993   BigQuery keyword            263 terms
common/                shared API, enrichment, server
archive/               old v5.0 / legacy / discovery / eval
```

**Golden rule:** collection and enrichment run only on Moody server `comm-cme-p01`. The laptop is for editing, Git, SSH, and BigQuery. Never download TikTok media locally. Secrets live in the server `.env` — never commit them, and never copy that file onto a laptop.

Each pipeline has its own API credentials, input list, BigQuery table, results, and logs. None of them write `tiktok_video_enriched`.

Shared enrichment lives in `common/scripts/enrich_pipeline.py`. Each runner calls it with **only that pipeline’s** `--pipeline` value (`content_creators`, `news`, or `keyword`) so credentials and BigQuery tables stay isolated without duplicating worker code.

After a **fully successful** daily run (collect + enrich/BQ + validate), the runner archives that run’s CSV to GCS. The object name is the research `--date` (same date overwrites). Partial or failed runs are skipped.

```text
gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv
gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv
gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv
```

Manual backfill / smoke (server, after `source .venv` + `.env`):

```bash
python common/scripts/upload_run_csv.py \
  --pipeline content_creators|news|keyword \
  --date YYYY-MM-DD \
  --file /path/to/that_run.csv
```

Details: [`docs/PIPELINES.md`](docs/PIPELINES.md) · [`docs/SERVER.md`](docs/SERVER.md).

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

P1 and P2 daily: OCR + emoji, Whisper skipped (backfill later). GCS after full success only.

**Automated P1 → P2** (not scheduled yet): `bash common/scripts/run_daily_p1_p2.sh` on `comm-cme-p01`. If `DATE` is unset it uses America/Chicago today minus `RESEARCH_LAG_DAYS` (default 2). P2 does not start if P1 fails. P3 is not in this job. Enable a clock time later with `common/server/install_p1_p2_schedule.sh` (do not edit pipeline flags).

P3’s daily default is the five-term sample (`news, trump, tsa, ice, netanyahu`). Drop `--sample` only after that sample is reviewed — the full 263-term list can exhaust the keyword quota.

After pulling on the server, keep the existing server `.env` in place (never commit or copy it).

Static check (laptop-safe, no API): `python common/scripts/validate_pipelines_static.py`

Docs: [`docs/PIPELINES.md`](docs/PIPELINES.md) · [`docs/SCHEMA.md`](docs/SCHEMA.md) · [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/SERVER.md`](docs/SERVER.md)
