"""Static checks for collection CSV export SQL (failed-handle stubs included)."""

from __future__ import annotations

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

from enrichment.bigquery_loader import BQ_SCHEMAS, CONTENT_CREATORS_TABLE
from tiktok.collection_csv import collection_export_sql, csv_export_fields


def main() -> int:
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
    sql = collection_export_sql(
        "cfme-mediaengagment-prod", "tiktok_research", "content_creators", fields
    )
    if "collection_status = 'ok'" in sql:
        print("FAIL export SQL still drops api_failed rows")
        return 1
    if not sql.strip().startswith(
        "SELECT collection_status, api_error_code, failure_reason"
    ):
        print("FAIL export SQL should lead with status columns")
        return 1
    if "api_failed" not in sql:
        print("FAIL export SQL should sort api_failed rows first")
        return 1
    print("PASS collection CSV includes handle API failures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
