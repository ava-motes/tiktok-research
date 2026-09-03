"""Write failed-handle stub rows into P1/P2 BigQuery tables (server only).

Reads checkpoints for a research date and upserts collection_status=api_failed
rows so frequent API failures are visible in content_creators / news.

    python scripts/backfill_handle_api_failures.py --date 2026-08-28
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from pathlib import Path
import importlib.util

def _setup_repo():
    for p in Path(__file__).resolve().parents:
        boot = p / "common" / "bootstrap.py"
        if boot.is_file():
            spec = importlib.util.spec_from_file_location("_tiktok_bootstrap", boot)
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            return mod.setup()
    raise RuntimeError("common/bootstrap.py not found")

ROOT = _setup_repo()

from tiktok.collection.server_guard import require_collection_server


def _failed_handles(path: str) -> list[str]:
    if not os.path.isfile(path):
        return []
    data = json.load(open(path, encoding="utf-8"))
    out = []
    seen = set()
    for key in data.get("failed") or []:
        handle = str(key).split("|", 1)[0].strip().lstrip("@").lower()
        if handle and handle not in seen:
            seen.add(handle)
            out.append(handle)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill P1/P2 failed-handle rows into BigQuery"
    )
    parser.add_argument("--date", required=True, help="Research date YYYY-MM-DD")
    parser.add_argument("--config", default="common/config.yaml")
    args = parser.parse_args()

    require_collection_server()

    from tiktok.config import load_config
    from enrichment.bigquery_loader import (
        _client,
        build_handle_api_failure_row,
        content_creators_table_id,
        ensure_content_creators_table,
        ensure_news_accounts_table,
        news_accounts_table_id,
        upsert_handle_api_failure_row,
    )
    from tiktok.collection.date_window import research_window

    from tiktok.pipelines import PIPELINE_CONTENT_CREATORS, PIPELINE_NEWS, get_pipeline

    cfg = load_config(args.config)
    tz = cfg.research_timezone or "America/Chicago"
    window = research_window(args.date, timezone_name=tz)
    p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    p2 = get_pipeline(cfg, PIPELINE_NEWS)
    jobs = [
        (
            "content_creators",
            os.path.join(
                p1.resolved_checkpoint_dir(cfg),
                f"content_creators_newsfluencer_combined_{args.date}.json",
            ),
            "CONTENT_CREATOR_API",
        ),
        (
            "news",
            os.path.join(
                p2.resolved_checkpoint_dir(cfg),
                f"news_news_{args.date}.json",
            ),
            "NEWS_API",
        ),
    ]

    ensure_content_creators_table()
    ensure_news_accounts_table()
    bq = _client()
    for table_id in (content_creators_table_id(), news_accounts_table_id()):
        bq.query(
            f"""
            UPDATE `{table_id}`
            SET collection_status = 'ok'
            WHERE collection_status IS NULL
              AND STARTS_WITH(IFNULL(video_id, ''), 'handle_fail:') = FALSE
            """
        ).result()
        print(f"marked existing videos ok: {table_id}", flush=True)

    jobs = [
        (
            "content_creators",
            os.path.join(
                ckpt_dir, f"content_creators_newsfluencer_combined_{args.date}.json"
            ),
            "CONTENT_CREATOR_API",
        ),
        (
            "news",
            os.path.join(ckpt_dir, f"news_news_{args.date}.json"),
            "NEWS_API",
        ),
    ]
    total = 0
    for pipeline_id, path, api_source in jobs:
        handles = _failed_handles(path)
        print(f"{pipeline_id}: {len(handles)} failed handles from {path}", flush=True)
        for handle in handles:
            row = build_handle_api_failure_row(
                pipeline_id=pipeline_id,
                handle=handle,
                collection_date=window.research_date,
                collection_window_start=window.collection_window_start,
                collection_window_end=window.collection_window_end,
                api_source=api_source,
                api_error_code="internal_error",
                failure_reason="video/query HTTP 500 after retries (checkpointed failed)",
            )
            n = upsert_handle_api_failure_row(row)
            total += n
            print(f"  upserted @{handle} n={n}", flush=True)
    print(f"done rows={total}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
