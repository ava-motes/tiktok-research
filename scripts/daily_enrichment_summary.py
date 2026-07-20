#!/usr/bin/env python3
"""Generate a daily enrichment summary (JSON + Markdown) for ops review.

Usage (on comm-cme-p01):
    python scripts/daily_enrichment_summary.py
    python scripts/daily_enrichment_summary.py --date 2026-07-16 --out data/daily
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import bigquery_configured, enriched_table_id
from tiktok.logging_setup import setup_logging


def _query_bq(day: str) -> Dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    summary_q = f"""
    SELECT
      COUNT(*) AS videos_enriched,
      COUNTIF(audio_available) AS whisper_ok,
      COUNTIF(IFNULL(frames_with_text, ocr_frames_processed) > 0) AS ocr_ok,
      COUNTIF(IFNULL(emoji_count, 0) > 0) AS videos_with_emoji,
      ROUND(AVG(IFNULL(ocr_latency_seconds, 0)), 2) AS avg_ocr_latency_s,
      ROUND(AVG(IFNULL(whisper_latency_seconds, 0)), 2) AS avg_whisper_latency_s,
      ROUND(AVG(IFNULL(enrichment_quality_score, 0)), 1) AS avg_quality_score,
      ROUND(SUM(IFNULL(vision_api_cost_estimate, 0)), 4) AS ocr_cost_usd,
      ROUND(SUM(IFNULL(whisper_cost_estimate, 0)), 4) AS whisper_cost_usd,
      ROUND(SUM(IFNULL(total_cost_estimate, 0)), 4) AS total_cost_usd,
      ROUND(AVG(IFNULL(total_cost_estimate, 0)), 6) AS avg_cost_per_video_usd
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
    params = [bigquery.ScalarQueryParameter("day", "STRING", day)]
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    summary = dict(list(client.query(summary_q, job_config=cfg).result())[0])
    failures = [dict(r) for r in client.query(fail_q, job_config=cfg).result()]
    quality = [dict(r) for r in client.query(quality_q, job_config=cfg).result()]
    dups = list(client.query(dup_q).result())[0]["duplicate_video_ids"]
    n = int(summary.get("videos_enriched") or 0)
    return {
        "day": day,
        "source": "bigquery",
        "table": table,
        "summary": summary,
        "rates": {
            "whisper_success_pct": round(100 * int(summary.get("whisper_ok") or 0) / n, 2) if n else None,
            "ocr_success_pct": round(100 * int(summary.get("ocr_ok") or 0) / n, 2) if n else None,
            "emoji_detection_pct": round(100 * int(summary.get("videos_with_emoji") or 0) / n, 2) if n else None,
        },
        "failure_reasons": failures,
        "quality_score_distribution": quality,
        "duplicate_video_ids": int(dups or 0),
        "cost_projection": {
            "daily_at_5k": round(float(summary.get("avg_cost_per_video_usd") or 0) * 5000, 2),
            "monthly_at_5k_day": round(float(summary.get("avg_cost_per_video_usd") or 0) * 5000 * 30, 2),
        },
    }


def _to_markdown(report: Dict[str, Any]) -> str:
    s = report.get("summary") or {}
    r = report.get("rates") or {}
    lines = [
        f"# Enrichment daily summary — {report.get('day')}",
        "",
        f"- Videos enriched: **{s.get('videos_enriched')}**",
        f"- OCR success: **{r.get('ocr_success_pct')}%**",
        f"- Whisper success: **{r.get('whisper_success_pct')}%**",
        f"- Videos with emoji: **{s.get('videos_with_emoji')}** ({r.get('emoji_detection_pct')}%)",
        f"- Avg OCR latency: {s.get('avg_ocr_latency_s')}s",
        f"- Avg Whisper latency: {s.get('avg_whisper_latency_s')}s",
        f"- Avg quality score: {s.get('avg_quality_score')}",
        f"- Cost total: ${s.get('total_cost_usd')} (avg ${s.get('avg_cost_per_video_usd')}/video)",
        f"- Duplicate video_ids: {report.get('duplicate_video_ids')}",
        "",
        "## Top failure reasons",
    ]
    fails = report.get("failure_reasons") or []
    if not fails:
        lines.append("- (none)")
    else:
        for f in fails[:15]:
            lines.append(f"- {f.get('reason')}: {f.get('n')}")
    lines.append("")
    lines.append("## Quality score distribution")
    for q in report.get("quality_score_distribution") or []:
        lines.append(f"- {q.get('score')}: {q.get('n')}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily enrichment summary")
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="enrichment_date (UTC YYYY-MM-DD)",
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
    print(json.dumps(report, indent=2, default=str))
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
