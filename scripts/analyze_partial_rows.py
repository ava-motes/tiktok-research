#!/usr/bin/env python3
"""Analyze partial / low-quality enrichment rows in BigQuery (+ SQLite attempts).

Usage (on comm-cme-p01 only):
    python scripts/analyze_partial_rows.py
    python scripts/analyze_partial_rows.py --out data/partial_rows_analysis.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.enrichment.bigquery_loader import (
    bigquery_configured,
    enrichment_quality_score,
    enriched_table_id,
    pipeline_logs_table_id,
)
from tiktok.logging_setup import setup_logging


def _compute_quality(row: Dict[str, Any]) -> int:
    stored = row.get("enrichment_quality_score")
    if stored is not None:
        try:
            return int(stored)
        except (TypeError, ValueError):
            pass
    return enrichment_quality_score(
        has_metadata=bool(row.get("video_id") and row.get("creator_username")),
        has_whisper=bool((row.get("whisper_transcript") or "").strip()),
        has_ocr=bool((row.get("cleaned_ocr_text") or row.get("ocr_text") or "").strip()),
        has_voice_to_text=bool((row.get("voice_to_text") or "").strip()),
        has_emoji=bool((row.get("emoji_characters") or "").strip()),
        has_sticker=bool((row.get("sticker_text") or "").strip()),
    )


def _sqlite_attempts(conn, video_id: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "transcription": [],
        "ocr": [],
        "emoji": [],
        "previous_attempts": 0,
    }
    try:
        rows = conn.execute(
            """SELECT worker, ok, error, started_at, ended_at, elapsed_seconds
               FROM enrichment_log WHERE video_id=? ORDER BY id""",
            (video_id,),
        ).fetchall()
    except Exception:
        return out
    for r in rows:
        d = dict(r)
        worker = (d.get("worker") or "unknown").strip()
        entry = {
            "ok": bool(int(d.get("ok") or 0)),
            "error": (d.get("error") or "").strip(),
            "started_at": d.get("started_at"),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        if worker in out:
            out[worker].append(entry)
        out["previous_attempts"] += 1
    # Latest transcript error from staging
    try:
        t = conn.execute(
            "SELECT status, error FROM video_transcripts WHERE video_id=?",
            (video_id,),
        ).fetchone()
        if t:
            out["transcript_status"] = t[0]
            out["transcript_error"] = t[1]
    except Exception:
        pass
    return out


def _retry_recommendation(
    *,
    missing: List[str],
    transcript_error: str,
    attempts: Dict[str, Any],
) -> Dict[str, Any]:
    steps = []
    priority = "C"
    if "whisper" in missing:
        priority = "A"
        err = (transcript_error or attempts.get("transcript_error") or "").lower()
        if "format_not_supported" in err or "format is not supported" in err:
            steps.append(
                "force_audio_extract_ffmpeg_wav_then_whisper"
            )
        elif "download" in err:
            steps.append("redownload_audio_then_whisper")
        else:
            steps.append("retry_whisper_force_wav")
    if "ocr" in missing:
        if priority != "A":
            priority = "B"
        steps.append("retry_vision_ocr_force")
    if "emoji" in missing:
        steps.append("rerun_emoji_extract_after_text_layers")
    steps.append("bq_upsert_by_video_id")
    return {
        "priority": priority,
        "steps": steps,
        "do_not_duplicate_bq_rows": True,
    }


def fetch_bq_candidates() -> List[Dict[str, Any]]:
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    q = f"""
    SELECT
      video_id,
      creator_username,
      enrichment_status,
      enrichment_quality_score,
      failure_reason,
      whisper_transcript,
      whisper_status,
      ocr_text,
      raw_ocr_text,
      cleaned_ocr_text,
      ocr_quality_score,
      emoji_characters,
      voice_to_text,
      pipeline_version,
      enrichment_date
    FROM `{table}`
    """
    return [dict(r) for r in client.query(q).result()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze partial/low-quality BQ rows")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--out", default="data/partial_rows_analysis.json")
    parser.add_argument(
        "--quality-threshold",
        type=int,
        default=90,
        help="Include rows with enrichment_quality_score below this",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1

    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])

    rows = fetch_bq_candidates()
    targets: List[Dict[str, Any]] = []
    for row in rows:
        q = _compute_quality(row)
        status = (row.get("enrichment_status") or "").lower()
        if status != "partial" and q >= args.quality_threshold:
            continue
        whisper = bool((row.get("whisper_transcript") or "").strip())
        ocr = bool(
            (row.get("cleaned_ocr_text") or row.get("ocr_text") or "").strip()
        )
        emoji = bool((row.get("emoji_characters") or "").strip())
        missing = []
        if not whisper:
            missing.append("whisper")
        if not ocr:
            missing.append("ocr")
        if not emoji:
            missing.append("emoji")
        attempts = _sqlite_attempts(conn, row["video_id"])
        tr_err = (
            (attempts.get("transcript_error") or "")
            or (row.get("failure_reason") or "")
        )
        # Prefer explicit whisper:* from failure_reason
        fr = row.get("failure_reason") or ""
        if "whisper:" in fr:
            tr_err = fr.split("whisper:", 1)[-1].split("|")[0].strip()
        rec = _retry_recommendation(
            missing=missing, transcript_error=tr_err, attempts=attempts
        )
        targets.append(
            {
                "video_id": row.get("video_id"),
                "creator_username": row.get("creator_username"),
                "enrichment_status": status,
                "quality_score": q,
                "missing_layers": {
                    "whisper": "whisper" in missing,
                    "OCR": "ocr" in missing,
                    "emoji": "emoji" in missing,
                },
                "failure_reason": fr or attempts.get("transcript_error") or "",
                "previous_attempts": attempts.get("previous_attempts") or 0,
                "attempt_detail": {
                    "transcription": attempts.get("transcription")[-3:],
                    "ocr": attempts.get("ocr")[-3:],
                    "emoji": attempts.get("emoji")[-3:],
                    "transcript_status": attempts.get("transcript_status"),
                    "transcript_error": attempts.get("transcript_error"),
                },
                "has_voice_to_text_fallback": bool(
                    (row.get("voice_to_text") or "").strip()
                ),
                "retry_recommendation": rec,
                "pipeline_version": row.get("pipeline_version"),
            }
        )

    # Priority sort: A (missing whisper) then B (missing OCR) then rest
    def sort_key(x: Dict[str, Any]):
        p = (x.get("retry_recommendation") or {}).get("priority") or "C"
        return (0 if p == "A" else 1 if p == "B" else 2, x.get("creator_username") or "")

    targets.sort(key=sort_key)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": "comm-cme-p01",
        "table": enriched_table_id(),
        "pipeline_logs_table": pipeline_logs_table_id(),
        "criteria": {
            "enrichment_status": "partial",
            "or_quality_score_lt": args.quality_threshold,
        },
        "totals": {
            "bq_rows_scanned": len(rows),
            "targets": len(targets),
            "priority_A_missing_whisper": sum(
                1
                for t in targets
                if (t.get("retry_recommendation") or {}).get("priority") == "A"
            ),
            "priority_B_missing_ocr": sum(
                1
                for t in targets
                if (t.get("retry_recommendation") or {}).get("priority") == "B"
            ),
        },
        "rows": targets,
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    print(json.dumps(report["totals"], indent=2))
    print(f"Wrote {args.out}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
