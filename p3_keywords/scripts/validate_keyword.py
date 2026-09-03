"""Summarize a Pipeline 3 run (collection + enrichment + BQ).

Does not call the TikTok API. Safe to run after collection on the server.
Confirms sample video_ids were not written to v5 / P1 / P2 tables.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

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

from tiktok.collection.date_window import research_window
from tiktok.config import load_config
from tiktok.db import get_connection, parse_matched_keywords
from enrichment.bigquery_loader import (
    CONTENT_CREATORS_TABLE,
    ENRICHED_TABLE,
    KEYWORD_SEARCH_TABLE,
    NEWS_ACCOUNTS_TABLE,
    bigquery_configured,
    build_keyword_search_row,
    count_enriched_rows,
    keyword_search_table_id,
)
from tiktok.logging_setup import setup_logging
from tiktok.pipelines import PIPELINE_KEYWORD, get_pipeline


def _read_ids(path: str) -> List[str]:
    if not path or not os.path.isfile(path):
        return []
    out: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            vid = line.strip()
            if vid and not vid.startswith("#"):
                out.append(vid)
    return out


def _bq_count(table_id: str, video_ids: List[str]) -> Dict[str, Any]:
    if not video_ids or not bigquery_configured():
        return {"configured": bigquery_configured(), "rows": None, "error": None}
    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        job = client.query(
            f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE video_id IN UNNEST(@vids)",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("vids", "STRING", video_ids)
                ]
            ),
        )
        n = int(list(job.result())[0]["n"])
        return {"configured": True, "rows": n, "error": None, "table": table_id}
    except Exception as e:
        return {"configured": True, "rows": None, "error": str(e), "table": table_id}


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline 3 validation summary")
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument("--date", required=True)
    parser.add_argument("--ids-file", default="")
    parser.add_argument("--collect-report", default="")
    parser.add_argument("--started-at", default="")
    parser.add_argument("--runtime-seconds", default="")
    parser.add_argument("--enrich-exit", default="")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full-list", action="store_true")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_KEYWORD)
    sample = bool(args.sample) and not args.full_list
    keywords = pipeline.resolve_keywords(cfg, sample=sample)
    window = research_window(args.date, timezone_name=cfg.research_timezone)
    video_ids = _read_ids(args.ids_file)

    collect: Dict[str, Any] = {}
    if args.collect_report and os.path.isfile(args.collect_report):
        with open(args.collect_report, encoding="utf-8") as f:
            collect = json.load(f)
        if not video_ids:
            video_ids = list(collect.get("collected_video_ids") or [])

    conn = get_connection(cfg.paths["database"])
    whisper_ok = whisper_fail = vtt_n = ocr_ok = ocr_fail = emoji_n = 0
    ocr_scores: List[float] = []
    enrich_fail_ids: List[str] = []
    merged_keywords = 0
    for vid in video_ids:
        row = build_keyword_search_row(conn, vid)
        if not row:
            enrich_fail_ids.append(vid)
            continue
        mk = row.get("matched_keywords") or parse_matched_keywords("")
        if len(mk) > 1:
            merged_keywords += 1
        if (row.get("voice_to_text") or "").strip():
            vtt_n += 1
        ws = (row.get("whisper_status") or "").lower()
        if ws == "ok":
            whisper_ok += 1
        elif ws == "failed":
            whisper_fail += 1
        os_ = (row.get("ocr_status") or "").lower()
        if os_ == "ok":
            ocr_ok += 1
        elif os_ == "failed":
            ocr_fail += 1
        q = row.get("ocr_quality_score")
        if q is not None:
            ocr_scores.append(float(q))
        if int(row.get("emoji_count") or 0) > 0 or (row.get("emoji_characters") or "").strip():
            emoji_n += 1
        if (row.get("enrichment_status") or "").lower() == "failed":
            enrich_fail_ids.append(vid)
    conn.close()

    bq = _bq_count(keyword_search_table_id(), video_ids) if bigquery_configured() else {
        "configured": False,
        "rows": None,
        "error": None,
    }
    leaked_to_v5 = leaked_p1 = leaked_p2 = None
    leak_error = None
    if video_ids and bigquery_configured():
        try:
            leaked_to_v5 = count_enriched_rows(video_ids)
            from enrichment.bigquery_loader import (
                content_creators_table_id,
                news_accounts_table_id,
            )

            leaked_p1 = _bq_count(content_creators_table_id(), video_ids).get("rows")
            leaked_p2 = _bq_count(news_accounts_table_id(), video_ids).get("rows")
        except Exception as e:
            leak_error = str(e)
    avg_ocr = round(sum(ocr_scores) / len(ocr_scores), 2) if ocr_scores else None
    ended = datetime.now(timezone.utc).isoformat()
    overall = "ok"
    if int(collect.get("api_failures") or 0) != 0:
        overall = "failed"
    if args.enrich_exit not in ("", "0"):
        overall = "failed"
    if leaked_to_v5 not in (None, 0):
        overall = "failed"
    summary = {
        "pipeline": PIPELINE_KEYWORD,
        "collection_date": window.research_date,
        "timezone": window.timezone_name,
        "collection_window_start": window.collection_window_start,
        "collection_window_end": window.collection_window_end,
        "start_time": args.started_at or collect.get("started_at"),
        "end_time": ended,
        "runtime_seconds": float(args.runtime_seconds)
        if args.runtime_seconds
        else None,
        "sample_mode": sample,
        "keywords": keywords,
        "keyword_count": len(keywords),
        "api_source": collect.get("api_source") or pipeline.resolved_api_source(),
        "videos_returned": collect.get("api_rows_seen"),
        "videos_outside_window": collect.get("outside_window"),
        "videos_stored": collect.get("inserted_new"),
        "upserted_existing": collect.get("upserted_existing"),
        "excluded_pipeline_1": collect.get("excluded_pipeline_1"),
        "excluded_pipeline_2": collect.get("excluded_pipeline_2"),
        "excluded_overlap": collect.get("excluded_overlap"),
        "unique_video_ids": len(set(video_ids)),
        "videos_with_merged_keywords": merged_keywords,
        "duplicates_removed": collect.get("duplicate_skips"),
        "api_failures": collect.get("api_failures"),
        "voice_to_text_coverage": vtt_n,
        "whisper_ok": whisper_ok,
        "whisper_failed": whisper_fail,
        "ocr_ok": ocr_ok,
        "ocr_failed": ocr_fail,
        "average_ocr_quality": avg_ocr,
        "emoji_videos_with_emoji": emoji_n,
        "enrichment_failures": len(set(enrich_fail_ids)),
        "enrichment_failed_video_ids": sorted(set(enrich_fail_ids)),
        "enrich_pipeline_exit": args.enrich_exit,
        "bigquery_table": KEYWORD_SEARCH_TABLE,
        "bigquery_rows_for_run": bq.get("rows"),
        "bigquery_error": bq.get("error"),
        "tiktok_video_enriched_rows_for_run": leaked_to_v5,
        "content_creators_rows_for_run": leaked_p1,
        "news_rows_for_run": leaked_p2,
        "leak_error": leak_error,
        "overall_status": overall,
        "quota_usage": "not_exposed_by_research_api",
        "v5_table": ENRICHED_TABLE,
        "p1_table": CONTENT_CREATORS_TABLE,
        "p2_table": NEWS_ACCOUNTS_TABLE,
        "max_videos_per_keyword": collect.get("max_videos_per_keyword"),
    }
    print(json.dumps(summary, indent=2), flush=True)

    out = args.out
    if not out:
        os.makedirs(pipeline.resolved_summary_dir(cfg), exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = os.path.join(
            pipeline.resolved_summary_dir(cfg), f"keyword_validation_{stamp}.json"
        )
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Wrote {out}", flush=True)
    return 0 if summary["overall_status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
