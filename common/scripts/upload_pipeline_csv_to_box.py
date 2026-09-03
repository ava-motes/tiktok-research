"""Export one pipeline collection-date from BigQuery and upload to UT Box.

    python scripts/upload_pipeline_csv_to_box.py --pipeline keyword --date 2026-08-15

Server only. Requires BigQuery ADC and Box credentials in ``.env``.
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

from tiktok.collection.server_guard import require_collection_server


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload YYYY-MM-DD.csv for one pipeline to the UT Box folder"
    )
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=("content_creators", "news", "keyword"),
    )
    parser.add_argument("--date", required=True, help="Collection date YYYY-MM-DD")
    parser.add_argument(
        "--csv",
        default="",
        help="Upload this local CSV as {date}.csv instead of exporting from BigQuery",
    )
    args = parser.parse_args()

    require_collection_server()

    from tiktok.box_delivery import deliver_pipeline_csv_to_box
    from tiktok.config import load_config
    from tiktok.logging_setup import setup_logging

    setup_logging()
    cfg = load_config(args.config)
    result = deliver_pipeline_csv_to_box(
        pipeline_id=args.pipeline,
        collection_date=args.date,
        box_cfg=getattr(cfg, "box", None) or {},
        csv_path=args.csv or None,
    )
    print(json.dumps(result, indent=2), flush=True)
    if result.get("skipped") and result.get("reason") == "missing_box_credentials":
        return 2
    if result.get("ok") is False or result.get("error"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
