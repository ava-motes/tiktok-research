"""Export one collection-date CSV from the pipeline BigQuery table.

Writes ``YYYY-MM-DD.csv`` under each pipeline's results CSV dir for GCS /
local review. Includes handle API-failure stubs alongside video rows.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from typing import Any, Dict, List, Sequence

logger = logging.getLogger(__name__)

PIPELINE_BQ_TABLE = {
    "content_creators": "content_creators",
    "news": "news",
    "keyword": "keyword",
}

# Leftmost CSV columns so failed-handle rows are obvious when the file opens.
CSV_LEAD_FIELDS = (
    "collection_status",
    "api_error_code",
    "failure_reason",
    "creator_username",
    "video_id",
    "collection_date",
)


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "|".join(_csv_cell(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def csv_export_fields(schema_fields: Sequence[str]) -> List[str]:
    """Schema columns with status / handle / video_id first for review."""
    names = [str(name) for name in schema_fields]
    lead = [name for name in CSV_LEAD_FIELDS if name in names]
    rest = [name for name in names if name not in set(lead)]
    return lead + rest


def collection_export_sql(
    project: str, dataset: str, table: str, fields: Sequence[str]
) -> str:
    """SQL for one collection_date: videos plus handle API-failure stubs.

    Failed handles are ``collection_status = 'api_failed'`` with
    ``api_error_code`` set. They are listed first so they are easy to verify.
    Filter ``collection_status = 'ok'`` for video-only analysis.
    """
    cols = ", ".join(csv_export_fields(fields))
    return (
        f"SELECT {cols} "
        f"FROM `{project}.{dataset}.{table}` "
        "WHERE collection_date = @d "
        "ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END, "
        "posted_at"
    )


def infer_collection_date(conn, video_ids: Sequence[str]) -> str:
    """Most common collection_date among these video ids."""
    counts: Dict[str, int] = {}
    ids = [v for v in video_ids if v]
    for i in range(0, len(ids), 400):
        chunk = ids[i : i + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT collection_date, COUNT(*) FROM videos "
            f"WHERE video_id IN ({placeholders}) GROUP BY 1",
            list(chunk),
        ).fetchall()
        for date, n in rows:
            if date:
                counts[str(date)] = counts.get(str(date), 0) + int(n)
    if not counts:
        return ""
    return max(counts, key=counts.get)


def export_bq_collection_csv(
    pipeline_id: str,
    collection_date: str,
    output_path: str,
) -> int:
    """Write BQ rows for one pipeline + collection_date to CSV. Returns row count."""
    from google.cloud import bigquery

    from enrichment.bigquery_loader import BQ_SCHEMAS, bq_dataset, gcp_project

    table = PIPELINE_BQ_TABLE.get(pipeline_id)
    if not table:
        raise ValueError(f"Unknown pipeline {pipeline_id}")
    fields = csv_export_fields([f["name"] for f in BQ_SCHEMAS[table]])
    project = gcp_project()
    dataset = bq_dataset()
    sql = collection_export_sql(project, dataset, table, fields)
    client = bigquery.Client(project=project)
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("d", "STRING", collection_date)
            ]
        ),
    )
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    n = 0
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in job.result(page_size=1000):
            writer.writerow({name: _csv_cell(row[name]) for name in fields})
            n += 1
    logger.info(
        "Exported %s BigQuery rows pipeline=%s date=%s → %s",
        n,
        pipeline_id,
        collection_date,
        output_path,
    )
    return n


def summarize_collection_csv(path: str) -> Dict[str, Any]:
    """Count rows in a dated collection CSV; require status/video_id columns."""
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    if os.path.getsize(path) <= 0:
        raise ValueError(f"CSV is empty: {path}")
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {path}")
        fields = [str(c) for c in reader.fieldnames]
        for required in ("collection_status", "video_id", "collection_date"):
            if required not in fields:
                raise ValueError(f"CSV missing required column {required!r}: {path}")
        total = 0
        api_failed = 0
        videos = 0
        for row in reader:
            if not any((v or "").strip() for v in row.values()):
                continue
            total += 1
            status = (row.get("collection_status") or "").strip()
            vid = (row.get("video_id") or "").strip()
            if status == "api_failed" or vid.startswith("handle_fail:"):
                api_failed += 1
            else:
                videos += 1
    return {
        "path": os.path.abspath(path),
        "rows": total,
        "video_rows": videos,
        "api_failed_rows": api_failed,
        "fieldnames": fields,
    }


def validate_collection_csv_for_upload(
    path: str,
    *,
    collection_date: str,
    min_api_failed_rows: int = 0,
) -> Dict[str, Any]:
    """Validate dated CSV before GCS upload; never silently drop known failures.

    ``min_api_failed_rows`` is unique failed handles, not query retry attempts.
    """
    summary = summarize_collection_csv(path)
    day = (collection_date or "").strip()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        mismatched = 0
        for row in reader:
            if not any((v or "").strip() for v in row.values()):
                continue
            got = (row.get("collection_date") or "").strip()
            if day and got and got != day:
                mismatched += 1
        if mismatched:
            raise ValueError(
                f"CSV has {mismatched} rows with collection_date != {day!r}: {path}"
            )
    if min_api_failed_rows > 0:
        if summary["rows"] <= 0:
            raise ValueError(
                f"CSV has no data rows but {min_api_failed_rows} unique failed "
                f"handles were recorded: {path}"
            )
        if summary["api_failed_rows"] < min_api_failed_rows:
            raise ValueError(
                f"CSV omitted failed handles: expected >= {min_api_failed_rows} "
                f"unique api_failed rows, found {summary['api_failed_rows']}: {path}"
            )
    return summary


def write_dated_collection_csv(
    *,
    pipeline_id: str,
    collection_date: str,
    export_dir: str,
    min_api_failed_rows: int = 0,
) -> Dict[str, Any]:
    """Export BQ → ``{export_dir}/{date}.csv`` and validate contents."""
    day = (collection_date or "").strip()
    if not day:
        raise ValueError("collection_date is required")
    out = os.path.join(export_dir, f"{day}.csv")
    rows = export_bq_collection_csv(pipeline_id, day, out)
    summary = validate_collection_csv_for_upload(
        out,
        collection_date=day,
        min_api_failed_rows=min_api_failed_rows,
    )
    if summary["rows"] != rows:
        raise ValueError(
            f"CSV row recount mismatch: export wrote {rows}, re-read {summary['rows']}"
        )
    summary["exported_rows"] = rows
    return summary
