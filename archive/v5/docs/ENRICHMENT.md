# Enrichment Pipeline (server-only)

**TikTok collection and enrichment run ONLY on:**

```
cme-user1@comm-cme-p01.moody.utexas.edu:~/tiktok_research
```

The Mac is for SSH / code editing only. Do **not** put TikTok credentials, the
production SQLite DB, or enrichment jobs on a laptop.

**GCP (OCR + analytics only):**

| Field | Value |
|-------|--------|
| Project ID | `cfme-mediaengagment-prod` |
| Project number | `1050243555369` |
| Dataset | `tiktok_research` |

GCP does **not** host TikTok collection. Vision API is called from the server;
BigQuery stores enrichment outputs.

---

## Architecture

```
                    TikTok Research API
                             |
                             v
              comm-cme-p01.moody.utexas.edu
                 (Collection + Processing VM)
                             |
                      SQLite Raw DB
                   (~56K+ videos today)
                             |
         ----------------------------------------
         |                   |                  |
         v                   v                  v
   Whisper Worker     Google Vision OCR    Emoji Worker
   (server CPU/API)   (cfme-mediaengagment-prod)
         |                   |                  |
         ----------------------------------------
                             |
                  Enriched SQLite Staging
                             |
                             v
              cfme-mediaengagment-prod
                     BigQuery
                  tiktok_research
              tiktok_video_enriched
            (one row per TikTok video)
```

Media policy: temporary download → process → **delete immediately**. Never retain
video/audio on disk.

---

## GCP setup (one-time)

### 1. Enable Vision API

Console → project `cfme-mediaengagment-prod` → APIs & Services → enable **Cloud Vision API**.

### 2. Create BigQuery dataset

```
Project:  cfme-mediaengagment-prod
Dataset:  tiktok_research
```

Tables (created by script or manually):

- `tiktok_video_enriched` — **final analytics table** (one row per video:
  metadata + transcript + combined OCR + emoji CLDR fields)

Do **not** create separate BigQuery tables `video_transcripts`, `video_ocr`,
or `video_emojis`. Those names remain SQLite staging only on the server.

### 3. Service account

IAM → Service Accounts → Create `tiktok-enrichment-worker`

Roles:

- BigQuery Data Editor
- BigQuery Job User
- Cloud Vision API User

Download JSON key, then:

```bash
# From your Mac (transfer only — do not process TikTok locally)
mkdir -p ~/keys_staging   # optional local staging before scp
scp tiktok-enrichment-worker.json \
  cme-user1@comm-cme-p01.moody.utexas.edu:/home/cme-user1/keys/
```

On the server:

```bash
mkdir -p ~/keys
chmod 600 ~/keys/tiktok-enrichment-worker.json
```

---

## Server `.env`

On `~/tiktok_research/.env` **on the server**:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/home/cme-user1/keys/tiktok-enrichment-worker.json
GCP_PROJECT=cfme-mediaengagment-prod
BIGQUERY_PROJECT=cfme-mediaengagment-prod
BIGQUERY_DATASET=tiktok_research
VISION_ENABLED=true

# VM is 4 CPU / ~3.5 GB / no GPU — keep Whisper small
WHISPER_BACKEND=openai
WHISPER_MODEL=base
```

---

## Install & run (on the server only)

```bash
ssh cme-user1@comm-cme-p01.moody.utexas.edu
cd ~/tiktok_research
source .venv/bin/activate
pip install -r requirements-enrichment.txt

# Create BigQuery dataset/tables in cfme-mediaengagment-prod
python scripts/enrich_pipeline.py --ensure-bq-schema

# Inspect / migrate final BigQuery schema
python scripts/enrich_pipeline.py --inspect-bq-schema

# Small batch test → tiktok_video_enriched
python scripts/enrich_pipeline.py --group batch_test --limit 6 --sync-bigquery

# Hardening sample (measure OCR/Whisper/BQ rates)
python scripts/enrich_pipeline.py --group sample --limit 100 --sync-bigquery

# Six research validation URLs
python scripts/validate_enrichment_6videos.py

# Emoji CLDR unit + live caption sample
python scripts/validate_emoji_extract.py --live-limit 20 --sync-bigquery
```

### OCR sampling

Default: **0%, 25%, 50%, 75%, 100%** of the video, then fill up to **8–12** frames.
Do not OCR every frame.

### Whisper

Prefer OpenAI Whisper API or `faster-whisper` with `tiny`/`base` + `int8`.
Do not run `large` models on this VM.

### Emoji

Extract from caption + hashtags + transcript + OCR text (SQLite staging),
then fold into `tiktok_video_enriched.emojis` / `emoji_names` /
`emoji_categories` (CLDR/Unicode names + categories).

---

## Expected validation (six URLs)

| Case | Account | Expect |
|------|---------|--------|
| 1 | @harryjsisson | transcript + on-screen OCR |
| 2 | @jaysworld411 | transcript + tweet screenshot OCR |
| 3 | @joeycontino2 | green-screen / Twitter OCR |
| 4 | @cnn | edited text + tweet OCR |
| 5 | @simpleblacktheory | stitch OCR |
| 6 | @pauletteonthemic | screenshots + audio/lyrics transcript |

---

## Isolation rules

| Machine | Allowed |
|---------|---------|
| `comm-cme-p01` | TikTok API, SQLite, Whisper, Vision calls, emoji, BQ upload |
| Mac / laptop | SSH, edit code, `scp` keys — **no** production TikTok runs |
| `cfme-mediaengagment-prod` | Vision API + BigQuery (+ optional GCS) only |

---

## Files

See `docs/ENRICHMENT_CHANGELOG.md` for incremental change history.
