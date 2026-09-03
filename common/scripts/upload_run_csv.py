#!/usr/bin/env python3
"""Upload one successful pipeline CSV to gs://tiktok_research_3.

Usage:
    python common/scripts/upload_run_csv.py \\
        --pipeline content_creators --date YYYY-MM-DD --file PATH.csv

Object name is the research date (replaces the same date on rerun):
    gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv
    gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv
    gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv
"""

from __future__ import annotations

import argparse
import json
import os
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

from tiktok.gcs_archive import (
    DEFAULT_BUCKET,
    DEFAULT_PROJECT,
    upload_run_csv,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Archive a completed P1/P2/P3 CSV to GCS (overwrite same date)"
    )
    parser.add_argument(
        "--pipeline",
        required=True,
        help="content_creators | news | keyword",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Research/run date YYYY-MM-DD (object name, not upload time)",
    )
    parser.add_argument("--file", required=True, help="Local CSV path")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("GCS_RUN_ARCHIVE_BUCKET") or DEFAULT_BUCKET,
        help=f"Bucket name (default {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT") or DEFAULT_PROJECT,
        help=f"GCP project (default {DEFAULT_PROJECT})",
    )
    args = parser.parse_args()
    try:
        result = upload_run_csv(
            pipeline_id=args.pipeline,
            research_date=args.date,
            csv_path=args.file,
            bucket=args.bucket,
            project=args.project,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps(result, indent=2), flush=True)
    print(f"Uploaded {result['gcs_uri']} ({result['bytes']} bytes)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
