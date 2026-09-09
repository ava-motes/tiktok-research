#!/usr/bin/env python3
"""Sequential P1 → P2 production job on comm-cme-p01.

Not scheduled. Computes a lagged research date unless DATE is set.
Does not run P3. Skips Whisper. GCS is uploaded by each runner
after that pipeline completes (API handle failures allowed).

    python common/scripts/run_daily_p1_p2.py --preflight
    python common/scripts/run_daily_p1_p2.py
    DATE=YYYY-MM-DD python common/scripts/run_daily_p1_p2.py
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO

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
from tiktok.config import load_config
from tiktok.p1_p2_daily import (
    P1_BQ_TABLE,
    P1_PIPELINE,
    P2_BQ_TABLE,
    P2_PIPELINE,
    assert_safe_argv,
    gcs_uri,
    p1_argv,
    p2_argv,
    resolve_lag_days,
    resolve_research_date,
)
from tiktok.pipelines import PIPELINE_CONTENT_CREATORS, PIPELINE_NEWS, get_pipeline


def _tee_write(files: List[TextIO], text: str) -> None:
    for fh in files:
        fh.write(text)
        fh.flush()


def _newest_json(directory: Path, pattern: str) -> Optional[Path]:
    if not directory.is_dir():
        return None
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _summarize_full_run(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"summary_path": str(path), "parse_error": True}
    keep = (
        "pipeline_id",
        "research_date",
        "stop_reason",
        "api_failures",
        "unique_collected_video_ids",
        "enrich_exit",
        "enrich_exit_codes",
        "validate_exit",
        "csv_paths",
        "csv_path",
        "ids_path",
        "ids_paths",
    )
    out = {k: data[k] for k in keep if k in data}
    out["summary_path"] = str(path)
    return out


def _run_pipeline(
    *,
    name: str,
    argv: List[str],
    env: Dict[str, str],
    logs: List[TextIO],
) -> int:
    assert_safe_argv(argv)
    cmd = [sys.executable, *argv]
    _tee_write(logs, f"\n======== {name} START {' '.join(cmd)} ========\n")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        _tee_write(logs, line)
    rc = int(proc.wait())
    _tee_write(logs, f"======== {name} EXIT {rc} ========\n")
    return rc


def _status_line(ok: bool, label: str, detail: str = "") -> str:
    flag = "OK" if ok else "FAIL"
    return f"  {flag:4} {label}" + (f"  {detail}" if detail else "")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sequential P1 then P2 daily job (no P3, skip Whisper)"
    )
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument(
        "--date",
        default="",
        help="Research date YYYY-MM-DD. Default: DATE env or Chicago today minus lag.",
    )
    parser.add_argument(
        "--lag-days",
        type=int,
        default=None,
        help="Days behind Chicago today when --date/DATE is unset (default 2)",
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Check wiring, print planned commands, do not collect or enrich",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    p2 = get_pipeline(cfg, PIPELINE_NEWS)
    date = resolve_research_date(args.date or None, lag_days=args.lag_days)
    lag = resolve_lag_days(args.lag_days)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    host = socket.gethostname()

    p1_log_dir = Path(p1.resolved_log_dir(cfg))
    p2_log_dir = Path(p2.resolved_log_dir(cfg))
    combo_log = p1_log_dir / f"p1_p2_daily_{date}_{stamp}.log"
    p1_log = p1_log_dir / f"p1_daily_{date}_{stamp}.log"
    p2_log = p2_log_dir / f"p2_daily_{date}_{stamp}.log"
    summary_path = (
        Path(p1.resolved_summary_dir(cfg)) / f"p1_p2_daily_{date}_{stamp}.json"
    )

    report: Dict[str, Any] = {
        "job": "p1_p2_daily",
        "host": host,
        "research_date": date,
        "lag_days": lag,
        "date_source": "explicit" if (args.date or os.environ.get("DATE")) else "lagged",
        "p3_included": False,
        "whisper": "skipped",
        "enrichment_steps": "ocr,emoji",
        "p1": {
            "pipeline": P1_PIPELINE,
            "script": p1_argv(date)[0],
            "argv": p1_argv(date),
            "bigquery": P1_BQ_TABLE,
            "gcs_uri": gcs_uri(P1_PIPELINE, date),
            "export_dir": p1.resolved_export_dir(cfg),
            "checkpoint_dir": p1.resolved_checkpoint_dir(cfg),
            "log_dir": str(p1_log_dir),
            "csv_dir": p1.resolved_export_dir(cfg),
        },
        "p2": {
            "pipeline": P2_PIPELINE,
            "script": p2_argv(date)[0],
            "argv": p2_argv(date),
            "bigquery": P2_BQ_TABLE,
            "gcs_uri": gcs_uri(P2_PIPELINE, date),
            "export_dir": p2.resolved_export_dir(cfg),
            "checkpoint_dir": p2.resolved_checkpoint_dir(cfg),
            "log_dir": str(p2_log_dir),
            "csv_dir": p2.resolved_export_dir(cfg),
        },
        "logs": {
            "combined": str(combo_log),
            "p1": str(p1_log),
            "p2": str(p2_log),
        },
    }

    header = [
        f"job=p1_p2_daily host={host} research_date={date} lag_days={lag}",
        f"P3=no Whisper=skip steps=ocr,emoji reset_checkpoints=no",
        f"P1 BQ={P1_BQ_TABLE}",
        f"P2 BQ={P2_BQ_TABLE}",
        f"P1 GCS={gcs_uri(P1_PIPELINE, date)}",
        f"P2 GCS={gcs_uri(P2_PIPELINE, date)}",
        f"combined_log={combo_log}",
        f"P1 cmd: python {' '.join(p1_argv(date))}",
        f"P2 cmd: python {' '.join(p2_argv(date))}",
    ]

    if args.preflight:
        print("\n".join(header), flush=True)
        print("PREFLIGHT: no collection, no enrichment, no GCS, no BigQuery writes", flush=True)
        print(json.dumps(report, indent=2), flush=True)
        return 0

    require_collection_server()

    p1_log_dir.mkdir(parents=True, exist_ok=True)
    p2_log_dir.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    lock_path = p1_log_dir / "p1_p2_daily.lock"
    lock_fh = lock_path.open("a+")
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"STOP: another p1_p2_daily job holds {lock_path}", flush=True)
        return 3

    t0 = time.perf_counter()
    started = datetime.now(timezone.utc).isoformat()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    overall = 0
    p1_rc: Optional[int] = None
    p2_rc: Optional[int] = None
    p2_skipped = ""

    with combo_log.open("w", encoding="utf-8") as combo, p1_log.open(
        "w", encoding="utf-8"
    ) as p1_fh, p2_log.open("w", encoding="utf-8") as p2_fh:
        _tee_write([combo, sys.stdout], "\n".join(header) + "\n")
        p1_before = _newest_json(
            Path(p1.resolved_summary_dir(cfg)), "content_creators_full_run_*.json"
        )
        p1_rc = _run_pipeline(
            name="P1",
            argv=p1_argv(date),
            env=env,
            logs=[combo, p1_fh, sys.stdout],
        )
        p1_after = _newest_json(
            Path(p1.resolved_summary_dir(cfg)), "content_creators_full_run_*.json"
        )
        if p1_after == p1_before:
            p1_after = None
        report["p1"]["exit"] = p1_rc
        report["p1"]["run"] = _summarize_full_run(p1_after)
        report["p1"]["collection_status"] = (
            "ok"
            if p1_rc == 0
            else ("failed" if p1_rc is not None else "not_run")
        )
        report["p1"]["gcs_upload"] = (
            "attempted_on_success" if p1_rc == 0 else "skipped_pipeline_failed"
        )

        if p1_rc != 0:
            p2_skipped = f"P1 exited {p1_rc}; P2 not started"
            overall = p1_rc or 1
            _tee_write(
                [combo, p2_fh, sys.stdout],
                f"SKIP P2: {p2_skipped}\n",
            )
        else:
            p2_before = _newest_json(
                Path(p2.resolved_summary_dir(cfg)), "news_full_run_*.json"
            )
            p2_rc = _run_pipeline(
                name="P2",
                argv=p2_argv(date),
                env=env,
                logs=[combo, p2_fh, sys.stdout],
            )
            p2_after = _newest_json(
                Path(p2.resolved_summary_dir(cfg)), "news_full_run_*.json"
            )
            if p2_after == p2_before:
                p2_after = None
            report["p2"]["exit"] = p2_rc
            report["p2"]["run"] = _summarize_full_run(p2_after)
            report["p2"]["collection_status"] = "ok" if p2_rc == 0 else "failed"
            report["p2"]["gcs_upload"] = (
                "attempted_on_success" if p2_rc == 0 else "skipped_pipeline_failed"
            )
            overall = p2_rc or 0

        report["p2"]["skipped"] = p2_skipped
        if p2_rc is None and p2_skipped:
            report["p2"]["collection_status"] = "skipped"
            report["p2"]["gcs_upload"] = "not_started"
        report["started_at"] = started
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        report["runtime_seconds"] = round(time.perf_counter() - t0, 3)
        report["overall_exit"] = overall
        report["overall_status"] = "ok" if overall == 0 else "failed"
        summary_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        lines = [
            "",
            "======== FINAL STATUS ========",
            _status_line(True, "research_date", date),
            _status_line(p1_rc == 0, "P1 collection+enrich+BQ+validate+GCS", f"exit={p1_rc}"),
            _status_line(
                p2_rc == 0 if p2_rc is not None else False,
                "P2 collection+enrich+BQ+validate+GCS",
                p2_skipped or f"exit={p2_rc}",
            ),
            f"  log  {combo_log}",
            f"  json {summary_path}",
            f"overall_exit={overall} overall_status={report['overall_status']}",
        ]
        _tee_write([combo, sys.stdout], "\n".join(lines) + "\n")

    p2_copy = p2_log_dir / summary_path.name
    try:
        p2_copy.write_text(summary_path.read_text(encoding="utf-8"), encoding="utf-8")
    except OSError:
        pass

    return overall


if __name__ == "__main__":
    raise SystemExit(main())
