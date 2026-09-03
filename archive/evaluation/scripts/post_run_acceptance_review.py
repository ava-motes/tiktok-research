#!/usr/bin/env python3
"""Compare a readiness report + BigQuery against production acceptance criteria.

Usage (after 500-video run):
    python scripts/post_run_acceptance_review.py \\
        --report data/production_readiness_500.json \\
        --sample 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import bigquery_configured, enriched_table_id
from tiktok.logging_setup import setup_logging

CRITERIA = [
    ("collection_success_pct", 99.0, ">="),
    ("ocr_success_pct", 98.0, ">="),
    ("whisper_success_pct", 98.0, ">="),
    ("bq_upload_success_pct", 100.0, ">="),
]


def _check_duplicates() -> int:
    if not bigquery_configured():
        return -1
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    q = f"""
    SELECT COUNT(*) AS n FROM (
      SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*) > 1
    )
    """
    return int(list(client.query(q).result())[0].n)


def _sample_rows(n: int) -> List[Dict[str, Any]]:
    if not bigquery_configured() or n <= 0:
        return []
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    # Columns aligned to enrichment-v5.0 schema (see docs/SCHEMA.md). Cost fields
    # live in tiktok_pipeline_logs, not the enriched table, so they are omitted here.
    q = f"""
    SELECT
      video_id, creator_username, enrichment_status, enrichment_quality_score,
      whisper_status, ocr_quality_score, ocr_character_count,
      SUBSTR(IFNULL(whisper_transcript, ''), 1, 120) AS transcript_preview,
      SUBSTR(IFNULL(ocr_text, ''), 1, 120) AS ocr_preview,
      emoji_characters, pipeline_version, enrichment_date
    FROM `{table}`
    WHERE pipeline_version LIKE 'enrichment-v5%'
       OR enrichment_date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    ORDER BY enrichment_date DESC
    LIMIT @n
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("n", "INT64", n)]
        ),
    )
    return [dict(r) for r in job.result()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default="data/production_readiness_500.json")
    parser.add_argument("--sample", type=int, default=5)
    parser.add_argument("--out", default="data/acceptance_review.json")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not os.path.isfile(args.report):
        print(f"Report not found: {args.report}", file=sys.stderr)
        return 1

    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)

    metrics = report.get("metrics") or {}
    checks = []
    all_pass = True
    for key, target, _op in CRITERIA:
        # map collection -> collection_success_pct or metadata
        val = metrics.get(key)
        if val is None and key == "collection_success_pct":
            val = metrics.get("collection_success_pct", metrics.get("metadata_success_pct"))
        ok = val is not None and float(val) >= target
        checks.append({"metric": key, "value": val, "target": target, "pass": ok})
        all_pass = all_pass and ok

    dups = _check_duplicates()
    dup_ok = dups == 0
    checks.append({"metric": "duplicate_rows", "value": dups, "target": 0, "pass": dup_ok})
    all_pass = all_pass and dup_ok

    samples = _sample_rows(args.sample)
    out = {
        "report_path": args.report,
        "production_ready_flag_in_report": report.get("production_ready"),
        "acceptance_checks": checks,
        "accepted": all_pass and bool(report.get("production_ready")),
        "cost": report.get("cost"),
        "quality_score": report.get("quality_score"),
        "failure_breakdown": report.get("failure_breakdown"),
        "avg_runtime_seconds_per_video": report.get("avg_runtime_seconds_per_video"),
        "bq_sample_rows": samples,
        "next_steps_if_accepted": [
            "Enable monitoring views (scripts/ensure_monitoring_views.py)",
            "Schedule daily incremental: enrich_pipeline.py --incremental --sync-bigquery",
            "Email/archive daily_enrichment_summary.py output",
            "Shift work to analytics / search / labeling / modeling",
        ],
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out["accepted"] else 2


if __name__ == "__main__":
    sys.exit(main())
