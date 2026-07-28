# Legacy (pre-v5.0) scripts

These are the original CSV-based collection/classification scripts from before the
`enrichment-v5.0` pipeline. They are **not** part of the production pipeline and
are kept only for reference/history.

**Do not use these for production.** Use the `tiktok/` package + `scripts/`
instead (see [`../README.md`](../README.md) and [`../docs/SCRIPTS.md`](../docs/SCRIPTS.md)).

## Contents

| File | Legacy role | Modern replacement |
|------|-------------|--------------------|
| `auth.py` | Standalone auth helper | `tiktok/auth.py` |
| `users.py` | Pull users → CSV | `tiktok/api/users.py`, `scripts/pull_user_info.py` |
| `videos.py` | Pull videos → CSV | `tiktok/api/videos.py`, `scripts/pull_videos.py` |
| `pull_videos.py` | Bulk video pull → CSV (hardcoded handles) | `scripts/pull_videos.py` (config-driven, SQLite) |
| `pull_followers.py` | Pull follower/user info → CSV | `scripts/pull_user_info.py` |
| `classify_videos.py` | OpenAI news/politics classify from CSV | `scripts/classify_videos.py` |
| `classify_accounts.py` | OpenAI account-type classify from CSV | `scripts/classify_accounts.py` |
| `export.py` | CSV writer helper | `scripts/export_csv.py`, `scripts/export_research_dataset.py` |
| `build_complete_handle_info.py` | Assemble handle info → CSV | `scripts/pull_user_info.py` + exports |
| `main.py` | Legacy entrypoint (pull videos + followers) | `scripts/enrich_pipeline.py`, `scripts/run_all.py` |

These modules import each other via root-level names (e.g. `from auth import ...`)
and are self-contained: no code under `tiktok/` or `scripts/` imports them.
