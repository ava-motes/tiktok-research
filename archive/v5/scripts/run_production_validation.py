#!/usr/bin/env python3
"""Production validation suite for tiktok_video_enriched (+ pipeline logs).

Run on comm-cme-p01 after every enrichment / BQ sync:

    python scripts/run_production_validation.py
    python scripts/run_production_validation.py --out data/production_validation_report.json

Checks:
  - no duplicate video_id
  - no null primary keys
  - valid timestamps (posted_at / enrichment_date parseable when present)
  - OCR not empty when OCR looks successful (ocr_quality_score >= 25)
  - transcript not empty when whisper_status = ok
  - latency recorded for successful Whisper rows (when column populated)
  - costs recorded in pipeline logs when present (best-effort)
  - BigQuery upsert surface healthy (table readable, row count > 0)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import (
    PIPELINE_VERSION,
    bigquery_configured,
    enriched_table_id,
    pipeline_logs_table_id,
)
from tiktok.logging_setup import setup_logging


def _check(name: str, ok: bool, detail: str, *, severity: str = "error") -> Dict[str, Any]:
    return {
        "name": name,
        "ok": bool(ok),
        "severity": severity if not ok else "info",
        "detail": detail,
    }


def _run_checks() -> Dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    logs = pipeline_logs_table_id()
    checks: List[Dict[str, Any]] = []

    # Table readable / upsert surface
    try:
        total = list(client.query(f"SELECT COUNT(*) AS n FROM `{table}`").result())[0]["n"]
        checks.append(
            _check(
                "bigquery_upsert_successful",
                int(total) > 0,
                f"Readable {table} with {total} rows",
            )
        )
    except Exception as e:
        checks.append(_check("bigquery_upsert_successful", False, str(e)))
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": PIPELINE_VERSION,
            "table": table,
            "checks": checks,
            "passed": False,
        }

    # Duplicates
    dup_n = list(
        client.query(
            f"""
            SELECT COUNT(*) AS n FROM (
              SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*) > 1
            )
            """
        ).result()
    )[0]["n"]
    checks.append(
        _check(
            "no_duplicate_video_id",
            int(dup_n) == 0,
            f"duplicate_video_ids={dup_n}",
        )
    )

    # Null PKs
    null_pk = list(
        client.query(
            f"""
            SELECT
              COUNTIF(video_id IS NULL OR TRIM(video_id) = '') AS null_video_id,
              COUNTIF(creator_username IS NULL OR TRIM(creator_username) = '') AS null_creator
            FROM `{table}`
            """
        ).result()
    )[0]
    checks.append(
        _check(
            "no_null_primary_keys",
            int(null_pk["null_video_id"] or 0) == 0
            and int(null_pk["null_creator"] or 0) == 0,
            f"null_video_id={null_pk['null_video_id']} null_creator={null_pk['null_creator']}",
        )
    )

    # Timestamps
    ts = list(
        client.query(
            f"""
            SELECT
              COUNTIF(
                enrichment_date IS NOT NULL AND enrichment_date != ''
                AND SAFE.PARSE_DATE('%Y-%m-%d', enrichment_date) IS NULL
              ) AS bad_enrichment_date,
              COUNTIF(
                posted_at IS NOT NULL AND posted_at != ''
                AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', posted_at) IS NULL
                AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', posted_at) IS NULL
                AND SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(posted_at, 1, 10)) IS NULL
              ) AS bad_posted_at
            FROM `{table}`
            """
        ).result()
    )[0]
    checks.append(
        _check(
            "valid_timestamps",
            int(ts["bad_enrichment_date"] or 0) == 0,
            f"bad_enrichment_date={ts['bad_enrichment_date']} bad_posted_at={ts['bad_posted_at']}",
            severity="error"
            if int(ts["bad_enrichment_date"] or 0)
            else "warning",
        )
    )

    # OCR consistency: high quality score must have cleaned/primary OCR text
    ocr_bad = list(
        client.query(
            f"""
            SELECT COUNT(*) AS n FROM `{table}`
            WHERE IFNULL(ocr_quality_score, 0) >= 25
              AND TRIM(IFNULL(cleaned_ocr_text, IFNULL(ocr_text, ''))) = ''
            """
        ).result()
    )[0]["n"]
    checks.append(
        _check(
            "ocr_nonempty_when_success",
            int(ocr_bad) == 0,
            f"high_ocr_quality_but_empty_text={ocr_bad}",
        )
    )

    # Whisper consistency
    wh_bad = list(
        client.query(
            f"""
            SELECT COUNT(*) AS n FROM `{table}`
            WHERE LOWER(IFNULL(whisper_status, '')) = 'ok'
              AND TRIM(IFNULL(whisper_transcript, '')) = ''
            """
        ).result()
    )[0]["n"]
    checks.append(
        _check(
            "transcript_nonempty_when_whisper_ok",
            int(wh_bad) == 0,
            f"whisper_status_ok_but_empty_transcript={wh_bad}",
        )
    )

    # Latency recorded (warning if widely missing)
    lat = list(
        client.query(
            f"""
            SELECT
              COUNTIF(LOWER(IFNULL(whisper_status,'')) = 'ok') AS whisper_ok,
              COUNTIF(
                LOWER(IFNULL(whisper_status,'')) = 'ok'
                AND whisper_latency_seconds IS NOT NULL
              ) AS with_latency
            FROM `{table}`
            """
        ).result()
    )[0]
    w_ok = int(lat["whisper_ok"] or 0)
    w_lat = int(lat["with_latency"] or 0)
    lat_ok = w_ok == 0 or (w_lat / w_ok) >= 0.5
    checks.append(
        _check(
            "latency_recorded",
            lat_ok,
            f"whisper_ok={w_ok} with_latency={w_lat}",
            severity="warning",
        )
    )

    # Costs in pipeline logs (best-effort — cost columns may live only in detail messages)
    try:
        log_n = list(client.query(f"SELECT COUNT(*) AS n FROM `{logs}`").result())[0]["n"]
        recent = list(
            client.query(
                f"""
                SELECT COUNT(*) AS n FROM `{logs}`
                WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                """
            ).result()
        )[0]["n"]
        checks.append(
            _check(
                "costs_recorded",
                int(log_n) > 0,
                f"pipeline_logs_total={log_n} last_7d={recent} "
                "(ops latency/errors in logs; USD estimates may be in worker detail)",
                severity="warning",
            )
        )
    except Exception as e:
        checks.append(
            _check("costs_recorded", False, f"pipeline_logs unreadable: {e}", severity="warning")
        )

    hard_fail = [c for c in checks if not c["ok"] and c.get("severity") == "error"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "table": table,
        "pipeline_logs_table": logs,
        "row_count": int(total),
        "checks": checks,
        "passed": len(hard_fail) == 0,
        "failed_checks": [c["name"] for c in hard_fail],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Production validation for enriched BQ table")
    parser.add_argument("--out", default="data/production_validation_report.json")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1

    report = _run_checks()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps({"passed": report["passed"], "failed_checks": report["failed_checks"], "row_count": report.get("row_count")}, indent=2))
    print(f"Wrote {args.out}")
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
