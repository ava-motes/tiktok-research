# Ava onboarding — Cursor + server + GCP/BigQuery

This guide is for **Ava** to run the TikTok research / enrichment project while Lahari is away.

**Golden rules**

1. **Do not run TikTok collection or enrichment on your laptop.**  
   Processing happens only on the Moody server: `comm-cme-p01`.
2. Your Mac/Cursor is for **editing code, SSH, and reading BigQuery**.
3. Secrets live in **server `.env`** and **GCP IAM** — never commit `.env` or JSON keys to GitHub.

---

## What you need access to

Ask Lahari / Ellery / boss to confirm you have:

| Access | What | Who grants |
|--------|------|------------|
| GitHub | Clone of this repo | Lahari (after code is pushed) |
| SSH | `cme-user1@comm-cme-p01.moody.utexas.edu` | Moody / CME IT or Lahari |
| GCP | Project `cfme-mediaengagment-prod` | Boss / GCP owner (Ellery) |
| BigQuery | Dataset `tiktok_research` | Same GCP grant |
| Secrets | Server `.env` already present, or values to fill | Lahari (offline handoff) |

**Recommended GCP roles for Ava (human login):**

- BigQuery Admin *or* BigQuery Data Viewer + Job User (minimum to explore data)
- Browser (so the project shows in the console picker)

You do **not** need the service-account JSON on your laptop if you only browse BigQuery in the console. The **server** already has the worker key for enrichment.

---

## Part A — Use Cursor AI with this project

### 1. Install Cursor

- Download Cursor: https://cursor.com  
- Sign in with your UT / work Google account if prompted.

### 2. Get the code (after Lahari pushes to GitHub)

```bash
# On your Mac
cd ~/Documents   # or wherever you keep projects
git clone <REPO_URL_LAHARI_WILL_SEND>
cd tiktok_research
```

Open that folder in Cursor: **File → Open Folder → `tiktok_research`**.

### 3. How to use Cursor effectively here

In Cursor chat / Agent, you can ask things like:

- “Show me how enrichment writes to BigQuery”
- “How do I run production validation on the server?”
- “What columns are in `tiktok_video_enriched`?”

Point Cursor at docs first:

- `docs/PIPELINE_ARCHITECTURE.md` — system map + column meanings  
- `docs/ENRICHMENT.md` — server + GCP setup  
- `docs/AVA_ONBOARDING.md` — this file  
- `data/proposed_quality_rubric.md` — quality scores  
- `CLAUDE.md` — project conventions (TikTok API, `.env`, pagination)

**Important:** Tell Cursor: *“Run TikTok / enrichment only via SSH on comm-cme-p01; do not download videos locally.”*

### 4. Optional: SSH from Cursor terminal

In Cursor’s terminal:

```bash
ssh cme-p01
# or
ssh cme-user1@comm-cme-p01.moody.utexas.edu
```

If hostname fails off campus, use UT VPN, then retry.

Suggested `~/.ssh/config` on your Mac:

```sshconfig
Host cme-p01
    HostName comm-cme-p01.moody.utexas.edu
    User cme-user1
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
    ServerAliveInterval 60
```

---

## Part B — Server setup (comm-cme-p01)

### First login

```bash
ssh cme-p01
cd ~/tiktok_research
ls
```

You should see `scripts/`, `tiktok/`, `config.yaml`, `.env` (not in git), `.venv/`.

### If the project is missing or you need a fresh clone on the server

```bash
ssh cme-p01
cd ~
git clone <REPO_URL> tiktok_research
cd tiktok_research
```

Then restore secrets (Lahari must provide these offline — **never email JSON keys in Slack if avoidable**):

```bash
# .env must exist on the server
nano ~/tiktok_research/.env   # or copy from Lahari’s secure handoff

# Service account key (already usually here)
ls -la ~/keys/tiktok-enrichment-worker.json
```

Minimum `.env` keys (see `.env.example`):

```bash
TIKTOK_CLIENT_KEY=...
TIKTOK_CLIENT_SECRET=...
OPENAI_API_KEY=...
GOOGLE_APPLICATION_CREDENTIALS=/home/cme-user1/keys/tiktok-enrichment-worker.json
GCP_PROJECT=cfme-mediaengagment-prod
BIGQUERY_PROJECT=cfme-mediaengagment-prod
BIGQUERY_DATASET=tiktok_research
VISION_ENABLED=true
WHISPER_BACKEND=openai
WHISPER_MODEL=base
```

### Create / refresh Python environment

```bash
cd ~/tiktok_research
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -r requirements-enrichment.txt   # if present
export PATH="$HOME/bin:$PATH"                # ffmpeg lives in ~/bin on this VM
```

Or use the helper:

```bash
chmod +x server/setup.sh
./server/setup.sh ~/tiktok_research
```

### Sanity checks

```bash
source .venv/bin/activate
set -a && source .env && set +a
python scripts/test_setup.py
python scripts/enrich_pipeline.py --inspect-bq-schema
python scripts/run_production_validation.py
```

---

## Part C — Access GCP + BigQuery (browser)

1. Go to https://console.cloud.google.com  
2. Sign in as **your UT Google account** (confirm with boss which email was granted — `@utexas.edu` vs `@austin.utexas.edu`).  
3. Select project: **`cfme-mediaengagment-prod`**  
4. Open **BigQuery** → dataset **`tiktok_research`**

### Tables that matter

| Table | Purpose |
|-------|---------|
| `tiktok_video_enriched` | **Main research table** — one row per video |
| `tiktok_pipeline_logs` | Ops / retries / errors (not for analysis) |

### Quick look query

```sql
SELECT
  COUNT(*) AS videos,
  COUNTIF(IFNULL(whisper_transcript,'') != '') AS with_whisper,
  COUNTIF(IFNULL(ocr_text,'') != '') AS with_ocr,
  COUNTIF(IFNULL(emoji_characters,'') != '') AS with_emoji
FROM `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`;
```

### Research fields to skim

`creator_username`, `caption`, `whisper_transcript`, `ocr_text`,  
`emoji_characters`, `emoji_descriptions`, `enrichment_status`, `pipeline_version`

Column meanings: `docs/PIPELINE_ARCHITECTURE.md`.

---

## Part D — Day-to-day work Ava may need to do

Always on the server:

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"
```

| Task | Command |
|------|---------|
| Pull latest code | `git pull` |
| Ensure BQ schema | `python scripts/enrich_pipeline.py --ensure-bq-schema` |
| Daily production job | `python scripts/enrich_pipeline.py --production --group <group>` |
| Enrich + sync (incremental) | `python scripts/enrich_pipeline.py --group batch_test --limit 50 --incremental --sync-bigquery --validate` |
| Validate only | `python scripts/run_production_validation.py` |
| Fix BQ consistency | `python scripts/fix_bq_consistency.py` |
| Export research CSV/Parquet | `python scripts/export_research_dataset.py` |
| Partial / quality audit | See `data/final_partial_audit.json` and `data/production_checklist.json` |

**Do not** use `--backfill-all-from-sqlite` on the full SQLite catalog unless you know it is **BQ-scoped** (script was fixed to only sync IDs already in BigQuery).

---

## Part E — Cursor + server workflow (recommended)

1. Edit code locally in Cursor (or ask Cursor Agent to edit).  
2. Commit / push to GitHub (or Lahari’s branch).  
3. On server: `git pull` then re-run the needed script.  
4. Confirm results in BigQuery console.

Deploy without git (if needed):

```bash
# From Mac project root
rsync -avz --exclude '.venv' --exclude '.git' --exclude 'audio' \
  ./ cme-p01:~/tiktok_research/
```

---

## Part F — Handoff checklist for Lahari → Ava

Before leave, Lahari should send Ava:

- [ ] GitHub repo URL + branch name  
- [ ] Confirmation Ava can `ssh cme-p01`  
- [ ] Confirmation Ava can open GCP project `cfme-mediaengagment-prod`  
- [ ] Note that server `.env` and `~/keys/tiktok-enrichment-worker.json` already exist (or secure copy)  
- [ ] This file: `docs/AVA_ONBOARDING.md`  
- [ ] Architecture: `docs/PIPELINE_ARCHITECTURE.md`  
- [ ] Production status: `data/production_checklist.json`

---

## Who to contact

| Issue | Contact |
|-------|---------|
| GCP / BigQuery access | Boss / Ellery (project owners) |
| SSH / Moody VM | CME IT / server admins |
| Pipeline code / enrichment | Cursor + this repo docs; escalate to Lahari when back |
| TikTok API credentials | Lahari / PI — do not regenerate casually |

---

## One-sentence summary for Ava

Clone the GitHub repo into Cursor, SSH to `comm-cme-p01` for all TikTok work, and use the GCP console on `cfme-mediaengagment-prod` → `tiktok_research.tiktok_video_enriched` to inspect the research data.
