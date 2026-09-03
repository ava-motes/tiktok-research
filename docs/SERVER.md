# Server

All TikTok collection and enrichment run on:

```text
cme-user1@comm-cme-p01.moody.utexas.edu:~/tiktok_research
```

Laptop: edit code, Git, SSH, browse BigQuery. Never put TikTok media or production `.env` on a laptop.

## One-time setup

Helpers live in `common/server/`:

- `setup.sh` — venv + deps on the VM
- `deploy_from_mac.sh` / `do_it_all.sh` / `grant_access.sh` — package and SSH
- `configure_gcp_env.sh` — append GCP/Vision/BQ vars without clobbering TikTok keys

The production `.env` already lives on the server. After a git pull, **keep that file in place** — do not move or copy it into the repo or onto a laptop. `.env.example` is only a key-name template for a brand-new VM.

Required keys:

- P1: `TIKTOK_CLIENT_KEY` / `TIKTOK_CLIENT_SECRET` (optional `CONTENT_CREATOR_TIKTOK_*`)
- P2: `NEWS_API_CLIENT_KEY` / `NEWS_API_CLIENT_SECRET`
- P3: `KEYWORD_SEARCH_API_CLIENT_KEY` / `KEYWORD_SEARCH_API_CLIENT_SECRET`
- Enrichment: `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT`, `OPENAI_API_KEY` (Whisper)
- Box (optional): `BOX_CLIENT_ID` / `BOX_CLIENT_SECRET`

GCP project: `cfme-mediaengagment-prod`, dataset `tiktok_research`.

Successful daily CSVs are archived (overwrite same `--date`) in the existing project bucket [tiktok_research_3](https://console.cloud.google.com/storage/browser/tiktok_research_3?project=cfme-mediaengagment-prod):

```text
gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv
gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv
gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv
```

Console/admin access is the Ellery GCP account (`ellery.ellis@utexas.edu`). Daily uploads on `comm-cme-p01` use the server’s existing `GOOGLE_APPLICATION_CREDENTIALS` (enrichment worker). Do not add GCS keys to git or a laptop `.env`.

If the worker cannot write objects, grant it on this bucket once:

```bash
gcloud storage buckets add-iam-policy-binding gs://tiktok_research_3 \
  --project=cfme-mediaengagment-prod \
  --member=serviceAccount:tiktok-enrichment-worker@cfme-mediaengagment-prod.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin
```

Smoke test (no TikTok collection):

```bash
python common/scripts/upload_run_csv.py \
  --pipeline content_creators --date 1900-01-01 --file /path/to/small.csv
gcloud storage ls -l gs://tiktok_research_3/p1_content_creators/
```

After SSH:

```bash
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"
python common/scripts/test_setup.py
```
