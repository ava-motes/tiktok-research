# P3 — Keywords

TikTok **client ID ending 993**. Dedicated keys: `KEYWORD_SEARCH_API_CLIENT_KEY` / `KEYWORD_SEARCH_API_CLIENT_SECRET` only. No fallback to P1 or P2.

| | |
|--|--|
| BigQuery table | `keyword` |
| Input | 263 keywords |
| Canonical list | `config/march_news_keywords.txt` |
| Sample terms | `news`, `trump`, `tsa`, `ice`, `netanyahu` |
| Extra lists | `config/aug15_six_terms.txt`, `config/phrase_smoke_test.txt` |
| Results | `results/csv/` · `results/parquet/` · `results/summaries/` |
| Logs / checkpoints | `logs/` · `logs/checkpoints/` |
| Local Box copies | `box/` (`YYYY-MM-DD.csv`) |
| GCS archive | `gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv` |
| Copy-paste SQL | `sql/keyword.sql` |

Creators already in P1 or P2 handle lists are excluded from P3 BigQuery rows.

Does **not** write `content_creators`, `news`, or `tiktok_video_enriched`.

`run_keyword.py` enriches by calling `common/scripts/enrich_pipeline.py --pipeline keyword` only. After a fully successful run it archives the date CSV via `common/scripts/upload_run_csv.py` (same date overwrites).

The canonical daily job **is** the five-term sample. Do **not** run the full 263-term list until that sample is reviewed.

Collection and enrichment run **only** on `comm-cme-p01`.

## Daily run (server)

```bash
ssh cme-p01
cd ~/tiktok_research
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/bin:$PATH"

DATE=YYYY-MM-DD

# Canonical daily: five-term sample, OCR + emoji, skip Whisper
python p3_keywords/scripts/run_keyword.py \
  --date "$DATE" --sample --utc-day --skip-whisper

# Full 263-term list (only after sample review)
python p3_keywords/scripts/run_keyword.py \
  --date "$DATE" --utc-day --skip-whisper
```

## Validation

```bash
python p3_keywords/scripts/validate_keyword.py --date "$DATE"
python p3_keywords/scripts/validate_keyword_search_static.py
python p3_keywords/scripts/validate_keyword_search_credentials.py
python common/scripts/validate_pipelines_static.py
```

Collect-only: `python p3_keywords/scripts/collect_keyword.py --date "$DATE" --sample`
