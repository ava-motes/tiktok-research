"""Static checks for Box folder name matching and collection CSV SQL."""

from __future__ import annotations

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

from tiktok.box_delivery import (
    csv_export_fields,
    collection_export_sql,
    match_pipeline_folder,
)
from enrichment.bigquery_loader import BQ_SCHEMAS, CONTENT_CREATORS_TABLE


def main() -> int:
    names = ["p3_key_words", "p2_news", "p1_content_creators"]
    p1 = match_pipeline_folder("content_creators", names)
    p2 = match_pipeline_folder("news", names)
    p3 = match_pipeline_folder("keyword", names)
    if p1 != "p1_content_creators":
        print("FAIL p1", p1)
        return 1
    if p2 != "p2_news":
        print("FAIL p2", p2)
        return 1
    if p3 != "p3_key_words":
        print("FAIL p3", p3)
        return 1
    aliases = ["Pipeline 1 - Content Creators", "News", "keyword"]
    if match_pipeline_folder("content_creators", aliases) != "Pipeline 1 - Content Creators":
        print("FAIL alias p1")
        return 1
    forced = match_pipeline_folder("keyword", names, configured="p3_key_words")
    if forced != "p3_key_words":
        print("FAIL configured", forced)
        return 1
    fields = [f["name"] for f in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]]
    ordered = csv_export_fields(fields)
    if ordered[:6] != [
        "collection_status",
        "api_error_code",
        "failure_reason",
        "creator_username",
        "video_id",
        "collection_date",
    ]:
        print("FAIL CSV lead columns", ordered[:6])
        return 1
    if set(ordered) != set(fields):
        print("FAIL CSV fields dropped or added")
        return 1
    sql = collection_export_sql("cfme-mediaengagment-prod", "tiktok_research", "content_creators", fields)
    if "collection_status = 'ok'" in sql:
        print("FAIL export SQL still drops api_failed rows")
        return 1
    if not sql.strip().startswith("SELECT collection_status, api_error_code, failure_reason"):
        print("FAIL export SQL should lead with status columns")
        return 1
    if "api_failed" not in sql:
        print("FAIL export SQL should sort api_failed rows first")
        return 1
    print("PASS Box folder matching")
    print("PASS Box CSV includes handle API failures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
