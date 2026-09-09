#!/usr/bin/env python3
"""Laptop-safe wiring checks for P1/P2 daily automation. No TikTok API."""

from __future__ import annotations

import os
import socket
import subprocess
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

from tiktok.config import load_config
from tiktok.gcs_archive import (
    DEFAULT_BUCKET,
    PIPELINE_GCS_PREFIX,
    gcs_object_uri,
    upload_run_csv_after_success,
)
from tiktok.p1_p2_daily import (
    P1_BQ_TABLE,
    P1_PIPELINE,
    P2_BQ_TABLE,
    P2_PIPELINE,
    assert_safe_argv,
    gcs_uri,
    p1_argv,
    p2_argv,
    resolve_research_date,
)
from tiktok.pipelines import (
    PIPELINE_CONTENT_CREATORS,
    PIPELINE_NEWS,
    get_pipeline,
)


def _ok(name: str, detail: str = "") -> None:
    extra = f"  {detail}" if detail else ""
    print(f"PASS  {name}{extra}")


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def main() -> int:
    cfg = load_config("common/config.yaml")
    p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    p2 = get_pipeline(cfg, PIPELINE_NEWS)
    date = resolve_research_date("2026-01-15")
    a1 = p1_argv(date)
    a2 = p2_argv(date)
    assert_safe_argv(a1)
    assert_safe_argv(a2)

    host = socket.gethostname()
    print(f"host={host}  (collection allowed only on comm-cme-p01)")

    for rel in (
        a1[0],
        a2[0],
        "common/scripts/enrich_pipeline.py",
        "common/scripts/upload_run_csv.py",
        "common/scripts/run_daily_p1_p2.py",
        "common/scripts/run_daily_p1_p2.sh",
    ):
        path = ROOT / rel
        if not path.is_file():
            _fail("paths", f"missing {rel}")
    _ok("1-4 runner/enrich/GCS/orchestrator files")

    if p1.bigquery_table != "content_creators":
        _fail("BQ", p1.bigquery_table)
    if p2.bigquery_table != "news":
        _fail("BQ", p2.bigquery_table)
    if P1_BQ_TABLE != "cfme-mediaengagment-prod.tiktok_research.content_creators":
        _fail("BQ", P1_BQ_TABLE)
    if P2_BQ_TABLE != "cfme-mediaengagment-prod.tiktok_research.news":
        _fail("BQ", P2_BQ_TABLE)
    _ok("6 BigQuery tables isolated", f"{P1_BQ_TABLE} | {P2_BQ_TABLE}")

    if DEFAULT_BUCKET != "tiktok_research_3":
        _fail("GCS", DEFAULT_BUCKET)
    if PIPELINE_GCS_PREFIX[P1_PIPELINE] != "p1_content_creators":
        _fail("GCS", "P1 prefix")
    if PIPELINE_GCS_PREFIX[P2_PIPELINE] != "p2_news":
        _fail("GCS", "P2 prefix")
    if gcs_uri(P1_PIPELINE, date) != "gs://tiktok_research_3/p1_content_creators/2026-01-15.csv":
        _fail("GCS", gcs_uri(P1_PIPELINE, date))
    if gcs_uri(P2_PIPELINE, date) != "gs://tiktok_research_3/p2_news/2026-01-15.csv":
        _fail("GCS", gcs_uri(P2_PIPELINE, date))
    if gcs_object_uri(P1_PIPELINE, date) != gcs_uri(P1_PIPELINE, date):
        _fail("GCS", "uri helper mismatch")
    _ok("4-5 GCS prefixes", gcs_uri(P1_PIPELINE, date))

    for pipe, label in ((p1, "P1"), (p2, "P2")):
        for attr in (
            "resolved_export_dir",
            "resolved_checkpoint_dir",
            "resolved_log_dir",
        ):
            resolved = getattr(pipe, attr)(cfg)
            if not resolved:
                _fail("paths", f"{label} {attr} empty")
    _ok("7-9 CSV/checkpoint/log dirs")

    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    for key in (
        "TIKTOK_CLIENT_KEY",
        "NEWS_API_CLIENT_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GCP_PROJECT",
    ):
        if key not in env_example:
            _fail("env", f".env.example missing {key}")
    _ok("10 env template names (no secrets)")

    from tiktok.collection.server_guard import require_collection_server

    if "cme-p01" in host.lower():
        _ok("11 server guard (already on collection host)")
    else:
        try:
            require_collection_server()
            _fail("11 server guard", "laptop was allowed")
        except SystemExit:
            _ok("11 server guard refuses this host")

    orch = (ROOT / "common/scripts/run_daily_p1_p2.py").read_text(encoding="utf-8")
    if "p2_skipped" not in orch or "P2 not started" not in orch:
        _fail("13 sequential stop", "P2 is not skipped when P1 fails")
    if "run_keyword" in orch:
        _fail("P3", "orchestrator references P3")
    _ok("12-13 sequential P1 then P2; P2 skipped if P1 fails")

    p1_src = (ROOT / a1[0]).read_text(encoding="utf-8")
    p2_src = (ROOT / a2[0]).read_text(encoding="utf-8")
    if "upload_run_csv_after_success" not in p1_src or "upload_run_csv_after_success" not in p2_src:
        _fail("14 GCS", "runner missing success-only upload")
    if p1_src.find("upload_run_csv_after_success") < p1_src.find("if val_rc != 0:"):
        _fail("14 GCS", "P1 upload before validate gate")
    if p2_src.find("upload_run_csv_after_success") < p2_src.find("if val_rc != 0:"):
        _fail("14 GCS", "P2 upload before validate gate")
    if "tiktok_video_enriched" in a1 or "tiktok_video_enriched" in a2:
        _fail("tables", "automation argv names tiktok_video_enriched")
    _ok("14 success-only GCS", str(upload_run_csv_after_success.__name__))

    for script in (a1[0], a2[0], "common/scripts/enrich_pipeline.py", "common/scripts/upload_run_csv.py"):
        cp = subprocess.run(
            [sys.executable, script, "--help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if cp.returncode != 0:
            _fail("15 --help", f"{script} exit {cp.returncode}: {cp.stderr[:400]}")
        if "tiktok_video_enriched" in (cp.stdout or "") and "never" not in (cp.stdout or "").lower():
            pass
    _ok("15 --help P1, P2, enrich, GCS uploader")

    cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or ""
    if cred and os.path.isfile(cred):
        _ok("5/6 GCP creds file present (IAM not queried here)")
    else:
        print("SKIP  5/6 GCS/BQ IAM (no GOOGLE_APPLICATION_CREDENTIALS on this machine)")

    print("PREFLIGHT complete (no TikTok quota used)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL  {e}", file=sys.stderr)
        raise SystemExit(1)
