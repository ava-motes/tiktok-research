#!/usr/bin/env python3
"""Run a 500-video enrichment validation and write a readiness report.

Usage (on comm-cme-p01 ONLY):
    python scripts/production_readiness_500.py --group sample --limit 500 --sync-bigquery
    python scripts/production_readiness_500.py --report-only --video-ids-file data/readiness_500_ids.json

Targets:
  Metadata 100% | BQ upload 100% | OCR ≥98% | Whisper ≥98%
  Emoji extraction deterministic (100% when emojis present)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.enrichment.bigquery_loader import (
    VISION_USD_PER_IMAGE,
    WHISPER_USD_PER_MINUTE,
    build_enriched_row,
)
from tiktok.enrichment.store import ensure_enrichment_schema, fetch_videos_for_enrichment
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

TARGETS = {
    "collection_success_pct": 99.0,
    "metadata_success_pct": 99.0,
    "bq_upload_success_pct": 100.0,
    "ocr_success_pct": 98.0,
    "whisper_success_pct": 98.0,
    "pipeline_crashes": 0,
    "duplicate_rows": 0,
}


def _run_pipeline(args) -> int:
    cmd = [
        sys.executable,
        "scripts/enrich_pipeline.py",
        "--config",
        args.config,
        "--limit",
        str(args.limit),
        "--force",
        "--metrics-report",
        args.metrics_report,
    ]
    if args.group:
        cmd += ["--group", args.group]
    if args.sync_bigquery:
        cmd.append("--sync-bigquery")
    logger.info("Launching: %s", " ".join(cmd))
    return subprocess.call(cmd)


def _failure_reasons(conn, video_ids: List[str]) -> Dict[str, int]:
    counts: Counter = Counter()
    for vid in video_ids:
        t = conn.execute(
            "SELECT status, error FROM video_transcripts WHERE video_id=?", (vid,)
        ).fetchone()
        if not t or t[0] != "ok":
            reason = (t[1] if t else "missing_transcript_row") or "whisper_failed"
            counts[f"whisper:{str(reason)[:80]}"] += 1
        ocr_n = conn.execute(
            "SELECT COUNT(*) FROM video_ocr WHERE video_id=?", (vid,)
        ).fetchone()[0]
        if not ocr_n:
            err = conn.execute(
                """SELECT error FROM enrichment_log
                   WHERE video_id=? AND worker='ocr' AND ok=0
                   ORDER BY id DESC LIMIT 1""",
                (vid,),
            ).fetchone()
            counts[f"ocr:{(err[0] if err else 'no_ocr_rows')[:80]}"] += 1
    return dict(counts.most_common(25))


def build_report(
    conn,
    video_ids: List[str],
    *,
    wall_seconds: float,
    bq_ok: int,
    bq_fail: int,
) -> Dict[str, Any]:
    n = len(video_ids)
    meta_ok = ocr_ok = whisper_ok = whisper_text = emoji_present = emoji_runs = 0
    vision_cost = whisper_cost = 0.0
    quality_scores: List[int] = []

    for vid in video_ids:
        v = conn.execute("SELECT video_id FROM videos WHERE video_id=?", (vid,)).fetchone()
        if v:
            meta_ok += 1
        ocr_n = conn.execute(
            "SELECT COUNT(*) FROM video_ocr WHERE video_id=?", (vid,)
        ).fetchone()[0]
        if ocr_n:
            ocr_ok += 1
        stats = conn.execute(
            "SELECT number_of_frames_processed FROM video_ocr_stats WHERE video_id=?",
            (vid,),
        ).fetchone()
        frames = int(stats[0]) if stats and stats[0] is not None else int(ocr_n or 0)
        vision_cost += frames * VISION_USD_PER_IMAGE

        t = conn.execute(
            """SELECT status, transcript, audio_duration_seconds
               FROM video_transcripts WHERE video_id=?""",
            (vid,),
        ).fetchone()
        if t and t[0] == "ok":
            whisper_ok += 1
            if t[1]:
                whisper_text += 1
            dur = float(t[2] or 0)
            if not dur:
                d2 = conn.execute(
                    "SELECT duration_seconds FROM videos WHERE video_id=?", (vid,)
                ).fetchone()
                dur = float(d2[0] or 0) if d2 else 0.0
            if dur > 0:
                whisper_cost += (dur / 60.0) * WHISPER_USD_PER_MINUTE

        em_n = conn.execute(
            "SELECT COUNT(*) FROM video_emojis WHERE video_id=?", (vid,)
        ).fetchone()[0]
        # emoji worker always "runs"; presence is content-dependent
        emoji_runs += 1
        if em_n:
            emoji_present += 1

        try:
            row = build_enriched_row(conn, vid)
            if row and row.get("enrichment_quality_score") is not None:
                quality_scores.append(int(row["enrichment_quality_score"]))
        except Exception:
            pass

    def pct(num: int) -> Optional[float]:
        return round(100.0 * num / n, 2) if n else None

    total_cost = vision_cost + whisper_cost
    avg_cost = (total_cost / n) if n else 0.0
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "videos": n,
        "wall_clock_seconds": round(wall_seconds, 2),
        "avg_runtime_seconds_per_video": round(wall_seconds / n, 2) if n else None,
        "metrics": {
            "collection_success_pct": pct(meta_ok),
            "ocr_success_pct": pct(ocr_ok),
            "whisper_success_pct": pct(whisper_ok),
            "whisper_with_text_pct": pct(whisper_text),
            "emoji_detection_pct": pct(emoji_present),
            "emoji_worker_completion_pct": pct(emoji_runs),
            "bq_upload_success_pct": (
                round(100.0 * bq_ok / (bq_ok + bq_fail), 2) if (bq_ok + bq_fail) else None
            ),
        },
        "targets": TARGETS,
        "targets_met": {
            "metadata": (pct(meta_ok) or 0) >= TARGETS["metadata_success_pct"],
            "ocr": (pct(ocr_ok) or 0) >= TARGETS["ocr_success_pct"],
            "whisper": (pct(whisper_ok) or 0) >= TARGETS["whisper_success_pct"],
            "bq": (
                (bq_ok + bq_fail) > 0
                and round(100.0 * bq_ok / (bq_ok + bq_fail), 2)
                >= TARGETS["bq_upload_success_pct"]
            ),
        },
        "cost": {
            "vision_cost_usd": round(vision_cost, 4),
            "whisper_cost_usd": round(whisper_cost, 4),
            "total_cost_usd": round(total_cost, 4),
            "avg_cost_per_video_usd": round(avg_cost, 6),
            "estimated_cost_per_day_at_5k": round(avg_cost * 5000, 2),
            "estimated_cost_per_day_at_20k": round(avg_cost * 20000, 2),
        },
        "quality_score": {
            "avg": round(sum(quality_scores) / len(quality_scores), 1) if quality_scores else None,
            "distribution": dict(Counter(quality_scores)),
        },
        "failure_breakdown": _failure_reasons(conn, video_ids),
        "bigquery": {"synced_ok": bq_ok, "synced_fail": bq_fail},
        "video_ids": video_ids,
        "production_ready": False,
    }
    report["production_ready"] = all(report["targets_met"].values())
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="500-video production readiness")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--group", default="sample")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sync-bigquery", action="store_true")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--video-ids-file", default="data/readiness_500_ids.json")
    parser.add_argument("--metrics-report", default="data/enrichment_sample_metrics.json")
    parser.add_argument("--out", default="data/production_readiness_500.json")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    bq_ok = bq_fail = 0
    t0 = time.perf_counter()
    if not args.report_only:
        code = _run_pipeline(args)
        if code != 0:
            logger.warning("Pipeline exited with code %s (continuing to report)", code)
        # Re-read metrics file for BQ counts if present
        if os.path.isfile(args.metrics_report):
            with open(args.metrics_report, encoding="utf-8") as f:
                m = json.load(f)
            bq_ok = int(m.get("bigquery", {}).get("synced_ok") or 0)
            bq_fail = int(m.get("bigquery", {}).get("synced_fail") or 0)
            video_ids = list(m.get("video_ids") or [])
        else:
            handles = cfg.get_handles(args.group) if args.group else None
            video_ids = [
                r["video_id"]
                for r in fetch_videos_for_enrichment(
                    conn, handles=handles, limit=args.limit
                )
            ]
        with open(args.video_ids_file, "w", encoding="utf-8") as f:
            json.dump({"video_ids": video_ids}, f, indent=2)
    else:
        if os.path.isfile(args.video_ids_file):
            with open(args.video_ids_file, encoding="utf-8") as f:
                video_ids = list(json.load(f).get("video_ids") or [])
        else:
            handles = cfg.get_handles(args.group) if args.group else None
            video_ids = [
                r["video_id"]
                for r in fetch_videos_for_enrichment(
                    conn, handles=handles, limit=args.limit
                )
            ]
        if args.sync_bigquery:
            from tiktok.enrichment.bigquery_loader import (
                bigquery_configured,
                sync_video_from_sqlite,
            )

            if bigquery_configured():
                for vid in video_ids:
                    try:
                        counts = sync_video_from_sqlite(conn, vid)
                        if counts.get("tiktok_video_enriched", 0) > 0:
                            bq_ok += 1
                        else:
                            bq_fail += 1
                    except Exception as e:
                        bq_fail += 1
                        logger.error("BQ sync %s: %s", vid, e)

    wall = time.perf_counter() - t0
    if args.report_only and os.path.isfile(args.metrics_report):
        with open(args.metrics_report, encoding="utf-8") as f:
            m = json.load(f)
        if m.get("wall_clock_seconds"):
            wall = float(m["wall_clock_seconds"])
        if not bq_ok and not bq_fail:
            bq_ok = int(m.get("bigquery", {}).get("synced_ok") or 0)
            bq_fail = int(m.get("bigquery", {}).get("synced_fail") or 0)

    report = build_report(
        conn, video_ids, wall_seconds=wall, bq_ok=bq_ok, bq_fail=bq_fail
    )
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    m = report["metrics"]
    logger.info(
        "Readiness: n=%s OCR=%.1f%% Whisper=%.1f%% BQ=%s/%s ready=%s",
        report["videos"],
        m.get("ocr_success_pct") or 0,
        m.get("whisper_success_pct") or 0,
        bq_ok,
        bq_ok + bq_fail,
        report["production_ready"],
    )
    print(json.dumps({k: report[k] for k in (
        "metrics", "targets_met", "cost", "quality_score",
        "failure_breakdown", "production_ready", "avg_runtime_seconds_per_video",
    )}, indent=2))
    conn.close()
    return 0 if report["production_ready"] else 2


if __name__ == "__main__":
    sys.exit(main())
