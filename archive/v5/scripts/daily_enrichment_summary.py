#!/usr/bin/env python3
"""Generate a daily enrichment summary (JSON + Markdown) for ops review.

Uses current tiktok_video_enriched schema (no legacy cost columns).
Cost figures are estimates from duration / OCR source counts.

Usage (on comm-cme-p01):
    python scripts/daily_enrichment_summary.py
    python scripts/daily_enrichment_summary.py --date 2026-07-16 --out data/daily
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import (
    VISION_USD_PER_IMAGE,
    WHISPER_USD_PER_MINUTE,
    bigquery_configured,
    enriched_table_id,
    pipeline_logs_table_id,
)
from tiktok.logging_setup import setup_logging


def _query_bq(day: str) -> Dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    logs = pipeline_logs_table_id()
    summary_q = f"""
    SELECT
      COUNT(*) AS videos_enriched,
      COUNTIF(
        LOWER(IFNULL(whisper_status, '')) = 'ok'
        AND TRIM(IFNULL(whisper_transcript, '')) != ''
      ) AS whisper_ok,
      COUNTIF(IFNULL(ocr_quality_score, 0) >= 25) AS ocr_ok,
      COUNTIF(TRIM(IFNULL(emoji_characters, '')) != '') AS videos_with_emoji,
      COUNTIF(LOWER(IFNULL(enrichment_status, '')) = 'failed') AS failed_rows,
      COUNTIF(LOWER(IFNULL(enrichment_status, '')) = 'partial') AS partial_rows,
      ROUND(AVG(IFNULL(whisper_latency_seconds, 0)), 2) AS avg_whisper_latency_s,
      ROUND(AVG(IFNULL(enrichment_quality_score, 0)), 1) AS avg_quality_score,
      ROUND(SUM(IFNULL(video_duration_seconds, 0) / 60.0 * {WHISPER_USD_PER_MINUTE}), 4)
        AS whisper_cost_usd_est,
      ROUND(SUM(IFNULL(ocr_source_count, 0) * {VISION_USD_PER_IMAGE}), 4)
        AS ocr_cost_usd_est
    FROM `{table}`
    WHERE enrichment_date = @day
    """
    fail_q = f"""
    SELECT IFNULL(NULLIF(failure_reason, ''), enrichment_status) AS reason, COUNT(*) AS n
    FROM `{table}`
    WHERE enrichment_date = @day
      AND (enrichment_status = 'failed'
           OR (failure_reason IS NOT NULL AND failure_reason != ''))
    GROUP BY reason
    ORDER BY n DESC
    LIMIT 25
    """
    quality_q = f"""
    SELECT enrichment_quality_score AS score, COUNT(*) AS n
    FROM `{table}`
    WHERE enrichment_date = @day
    GROUP BY score
    ORDER BY score DESC
    """
    dup_q = f"""
    SELECT COUNT(*) AS duplicate_video_ids FROM (
      SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*) > 1
    )
    """
    logs_q = f"""
    SELECT
      COUNT(*) AS log_events,
      COUNTIF(LOWER(IFNULL(status, '')) != 'ok') AS log_errors
    FROM `{logs}`
    WHERE DATE(created_at) = @day
       OR STARTS_WITH(IFNULL(created_at, ''), @day)
    """
    params = [bigquery.ScalarQueryParameter("day", "STRING", day)]
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    summary = dict(list(client.query(summary_q, job_config=cfg).result())[0])
    failures = [dict(r) for r in client.query(fail_q, job_config=cfg).result()]
    quality = [dict(r) for r in client.query(quality_q, job_config=cfg).result()]
    dups = list(client.query(dup_q).result())[0]["duplicate_video_ids"]
    try:
        log_row = dict(list(client.query(logs_q, job_config=cfg).result())[0])
    except Exception:
        log_row = {"log_events": None, "log_errors": None}
    n = int(summary.get("videos_enriched") or 0)
    whisper_cost = float(summary.get("whisper_cost_usd_est") or 0)
    ocr_cost = float(summary.get("ocr_cost_usd_est") or 0)
    total_cost = whisper_cost + ocr_cost
    avg_cost = (total_cost / n) if n else 0.0
    return {
        "day": day,
        "source": "bigquery",
        "table": table,
        "summary": {
            **summary,
            "total_cost_usd_est": round(total_cost, 4),
            "avg_cost_per_video_usd": round(avg_cost, 6),
            **log_row,
        },
        "rates": {
            "whisper_success_pct": round(
                100 * int(summary.get("whisper_ok") or 0) / n, 2
            )
            if n
            else None,
            "ocr_success_pct": round(100 * int(summary.get("ocr_ok") or 0) / n, 2)
            if n
            else None,
            "emoji_detection_pct": round(
                100 * int(summary.get("videos_with_emoji") or 0) / n, 2
            )
            if n
            else None,
        },
        "failure_reasons": failures,
        "quality_score_distribution": quality,
        "duplicate_video_ids": int(dups or 0),
        "cost_projection": {
            "daily_at_5k": round(avg_cost * 5000, 2),
            "monthly_at_5k_day": round(avg_cost * 5000 * 30, 2),
        },
    }


def _to_markdown(report: Dict[str, Any]) -> str:
    s = report.get("summary") or {}
    r = report.get("rates") or {}
    lines = [
        f"# Enrichment daily summary — {report.get('day')}",
        "",
        f"- Videos enriched: **{s.get('videos_enriched')}**",
        f"- Whisper ok: {s.get('whisper_ok')} ({r.get('whisper_success_pct')}%)",
        f"- OCR ok: {s.get('ocr_ok')} ({r.get('ocr_success_pct')}%)",
        f"- With emoji: {s.get('videos_with_emoji')} ({r.get('emoji_detection_pct')}%)",
        f"- Partial / failed: {s.get('partial_rows')} / {s.get('failed_rows')}",
        f"- Avg quality: {s.get('avg_quality_score')}",
        f"- Avg Whisper latency (s): {s.get('avg_whisper_latency_s')}",
        f"- Est. cost USD: {s.get('total_cost_usd_est')} "
        f"(Whisper {s.get('whisper_cost_usd_est')}, OCR {s.get('ocr_cost_usd_est')})",
        f"- Duplicate video_ids (table-wide): {report.get('duplicate_video_ids')}",
        f"- Pipeline log events / errors: {s.get('log_events')} / {s.get('log_errors')}",
        "",
        "## Failure reasons",
    ]
    for row in report.get("failure_reasons") or []:
        lines.append(f"- {row.get('reason')}: {row.get('n')}")
    if not report.get("failure_reasons"):
        lines.append("- (none)")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily enrichment ops summary")
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="UTC enrichment_date YYYY-MM-DD",
    )
    parser.add_argument("--out", default="data/daily")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1

    report = _query_bq(args.date)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(args.out, exist_ok=True)
    json_path = os.path.join(args.out, f"enrichment_summary_{args.date}.json")
    md_path = os.path.join(args.out, f"enrichment_summary_{args.date}.md")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(_to_markdown(report))
    print(json.dumps({"json": json_path, "md": md_path, "summary": report["summary"]}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
