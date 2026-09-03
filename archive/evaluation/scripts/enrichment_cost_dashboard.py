#!/usr/bin/env python3
"""Cost dashboard for Vision + Whisper enrichment estimates.

Reads from BigQuery ``tiktok_video_enriched`` (preferred) or local SQLite staging.

Usage (on comm-cme-p01):
    python scripts/enrichment_cost_dashboard.py
    python scripts/enrichment_cost_dashboard.py --days 7 --out data/enrichment_cost_dashboard.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.enrichment.bigquery_loader import (
    VISION_USD_PER_IMAGE,
    WHISPER_USD_PER_MINUTE,
    bigquery_configured,
    enriched_table_id,
    ensure_dataset_and_tables,
)
from tiktok.logging_setup import setup_logging


def _from_bigquery(days: int) -> Dict[str, Any]:
    from google.cloud import bigquery

    ensure_dataset_and_tables()
    client = bigquery.Client()
    table = enriched_table_id()
    q = f"""
    SELECT
      COUNT(*) AS videos_processed,
      SUM(IFNULL(vision_api_cost_estimate, 0)) AS ocr_cost,
      SUM(IFNULL(whisper_cost_estimate, 0)) AS whisper_cost,
      SUM(IFNULL(total_cost_estimate, 0)) AS total_cost,
      AVG(IFNULL(total_cost_estimate, 0)) AS avg_cost_per_video,
      AVG(IFNULL(duration_seconds, 0)) AS avg_duration_seconds,
      AVG(IFNULL(number_of_frames_processed, 0)) AS avg_frames
    FROM `{table}`
    WHERE enrichment_date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY))
    """
    daily_q = f"""
    SELECT
      enrichment_date AS day,
      COUNT(*) AS videos_processed,
      SUM(IFNULL(vision_api_cost_estimate, 0)) AS ocr_cost,
      SUM(IFNULL(whisper_cost_estimate, 0)) AS whisper_cost,
      SUM(IFNULL(total_cost_estimate, 0)) AS total_cost,
      AVG(IFNULL(total_cost_estimate, 0)) AS avg_cost_per_video
    FROM `{table}`
    WHERE enrichment_date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY))
    GROUP BY enrichment_date
    ORDER BY enrichment_date DESC
    """
    params = [bigquery.ScalarQueryParameter("days", "INT64", days)]
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    summary = dict(list(client.query(q, job_config=cfg).result())[0])
    daily = [dict(r) for r in client.query(daily_q, job_config=cfg).result()]
    return {"source": "bigquery", "table": table, "summary": summary, "daily": daily}


def _from_sqlite(conn, days: int) -> Dict[str, Any]:
    # Approximate from staging when BQ unavailable
    vids = [
        r[0]
        for r in conn.execute(
            """SELECT video_id FROM videos
               WHERE inserted_at >= datetime('now', ?)
               ORDER BY inserted_at DESC""",
            (f"-{days} days",),
        ).fetchall()
    ]
    ocr_cost = whisper_cost = 0.0
    per_day: Dict[str, Dict[str, float]] = {}
    for vid in vids:
        frames = conn.execute(
            "SELECT number_of_frames_processed FROM video_ocr_stats WHERE video_id=?",
            (vid,),
        ).fetchone()
        n_frames = int(frames[0]) if frames and frames[0] is not None else 0
        if not n_frames:
            n_frames = conn.execute(
                "SELECT COUNT(*) FROM video_ocr WHERE video_id=?", (vid,)
            ).fetchone()[0]
        vcost = n_frames * VISION_USD_PER_IMAGE
        dur = conn.execute(
            """SELECT COALESCE(audio_duration_seconds, 0) FROM video_transcripts
               WHERE video_id=? AND status='ok'""",
            (vid,),
        ).fetchone()
        if not dur or not dur[0]:
            d2 = conn.execute(
                "SELECT duration_seconds FROM videos WHERE video_id=?", (vid,)
            ).fetchone()
            dur_s = float(d2[0] or 0) if d2 else 0.0
        else:
            dur_s = float(dur[0] or 0)
        wcost = (dur_s / 60.0) * WHISPER_USD_PER_MINUTE if dur_s > 0 else 0.0
        ocr_cost += vcost
        whisper_cost += wcost
        day = (
            conn.execute(
                "SELECT substr(inserted_at,1,10) FROM videos WHERE video_id=?", (vid,)
            ).fetchone()
            or [""]
        )[0]
        bucket = per_day.setdefault(
            day or "unknown",
            {"videos_processed": 0, "ocr_cost": 0.0, "whisper_cost": 0.0, "total_cost": 0.0},
        )
        bucket["videos_processed"] += 1
        bucket["ocr_cost"] += vcost
        bucket["whisper_cost"] += wcost
        bucket["total_cost"] += vcost + wcost

    n = len(vids)
    total = ocr_cost + whisper_cost
    daily: List[Dict[str, Any]] = []
    for day, b in sorted(per_day.items(), reverse=True):
        daily.append(
            {
                "day": day,
                **b,
                "avg_cost_per_video": (b["total_cost"] / b["videos_processed"])
                if b["videos_processed"]
                else 0,
            }
        )
    return {
        "source": "sqlite",
        "summary": {
            "videos_processed": n,
            "ocr_cost": round(ocr_cost, 6),
            "whisper_cost": round(whisper_cost, 6),
            "total_cost": round(total, 6),
            "avg_cost_per_video": round(total / n, 6) if n else 0,
        },
        "daily": daily,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrichment cost dashboard")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--out", default="data/enrichment_cost_dashboard.json")
    parser.add_argument("--sqlite-only", action="store_true")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])

    if not args.sqlite_only and bigquery_configured():
        try:
            report = _from_bigquery(args.days)
        except Exception as e:
            report = _from_sqlite(conn, args.days)
            report["bq_error"] = str(e)[:300]
    else:
        report = _from_sqlite(conn, args.days)

    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["price_assumptions"] = {
        "vision_usd_per_image": VISION_USD_PER_IMAGE,
        "whisper_usd_per_minute": WHISPER_USD_PER_MINUTE,
        "note": "List-price estimates for planning, not GCP/OpenAI invoices",
    }
    # Extrapolations for production planning
    avg = float(report["summary"].get("avg_cost_per_video") or 0)
    report["projections"] = {
        "cost_per_100k_videos": round(avg * 100_000, 2),
        "cost_per_1m_videos": round(avg * 1_000_000, 2),
        "cost_per_day_at_5k": round(avg * 5000, 2),
        "cost_per_day_at_20k": round(avg * 20000, 2),
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
