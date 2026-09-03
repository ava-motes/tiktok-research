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

Test the reorganized script paths on this host before considering the layout final. Do not commit from the laptop until that server check has run.

After SSH:

```bash
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"
python common/scripts/test_setup.py
```
