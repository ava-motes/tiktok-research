"""Archive a completed pipeline CSV to GCS.

Object names use the research/run date, not the upload timestamp.
Re-uploading the same date replaces that object.

    gs://tiktok_research_3/p1_content_creators/YYYY-MM-DD.csv
    gs://tiktok_research_3/p2_news/YYYY-MM-DD.csv
    gs://tiktok_research_3/p3_keywords/YYYY-MM-DD.csv
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
from typing import Any, Dict, Iterable, List, Optional, Sequence

from tiktok.pipelines import PIPELINE_ID_ALIASES

DEFAULT_BUCKET = "tiktok_research_3"
DEFAULT_PROJECT = "cfme-mediaengagment-prod"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

PIPELINE_GCS_PREFIX = {
    "content_creators": "p1_content_creators",
    "news": "p2_news",
    "keyword": "p3_keywords",
}

_PIPELINE_SHORT = {
    "p1": "content_creators",
    "p1_content_creators": "content_creators",
    "p2": "news",
    "p2_news": "news",
    "p3": "keyword",
    "p3_keywords": "keyword",
    "p3_key_words": "keyword",
}


def canonicalize_pipeline_id(pipeline_id: str) -> str:
    raw = (pipeline_id or "").strip()
    if not raw:
        raise ValueError("pipeline is required")
    key = PIPELINE_ID_ALIASES.get(raw, raw)
    key = _PIPELINE_SHORT.get(key, key)
    if key not in PIPELINE_GCS_PREFIX:
        raise ValueError(
            f"Unknown pipeline {pipeline_id!r}. Use content_creators, news, or keyword."
        )
    return key


def gcs_object_uri(pipeline_id: str, research_date: str, bucket: str = DEFAULT_BUCKET) -> str:
    pid = canonicalize_pipeline_id(pipeline_id)
    date = validate_research_date(research_date)
    prefix = PIPELINE_GCS_PREFIX[pid]
    return f"gs://{bucket.strip().removeprefix('gs://').rstrip('/')}/{prefix}/{date}.csv"


def validate_research_date(research_date: str) -> str:
    date = (research_date or "").strip()
    if not DATE_RE.match(date):
        raise ValueError(f"date must be YYYY-MM-DD, got {research_date!r}")
    return date


def validate_local_csv(path: str) -> int:
    """Return data-row count (excluding header). Raise ValueError if unusable."""
    if not path:
        raise ValueError("CSV path is empty")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    if os.path.getsize(path) <= 0:
        raise ValueError(f"CSV is empty: {path}")
    with open(path, newline="", encoding="utf-8-sig") as f:
        try:
            rows = list(csv.reader(f))
        except csv.Error as e:
            raise ValueError(f"CSV is not readable: {path} ({e})") from e
    nonempty = [r for r in rows if any((c or "").strip() for c in r)]
    if not nonempty:
        raise ValueError(f"CSV has no header or rows: {path}")
    return max(len(nonempty) - 1, 0)


def resolve_run_csv_file(
    cfg,
    pipeline,
    research_date: str,
    csv_paths: Optional[Sequence[str]] = None,
) -> str:
    """Prefer the date-named Box/local CSV, then collection export CSVs."""
    date = validate_research_date(research_date)
    candidates: List[str] = []
    box_dir = pipeline.resolved_box_dir(cfg) if pipeline is not None else ""
    if box_dir:
        candidates.append(os.path.join(box_dir, f"{date}.csv"))
    for p in csv_paths or []:
        if p:
            candidates.append(p)
    seen = set()
    for path in candidates:
        ap = os.path.abspath(path)
        if ap in seen:
            continue
        seen.add(ap)
        if os.path.isfile(ap) and os.path.getsize(ap) > 0:
            validate_local_csv(ap)
            return ap
    raise FileNotFoundError(
        f"No non-empty CSV for pipeline={pipeline.id if pipeline else '?'} "
        f"date={date}. Looked in: {candidates}"
    )


def _gcloud_cmd(args: List[str]) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def _upload_with_storage_client(local: str, uri: str, project: str) -> Dict[str, Any]:
    from google.cloud import storage

    if not uri.startswith("gs://"):
        raise ValueError(uri)
    rest = uri[5:]
    bucket_name, _, blob_name = rest.partition("/")
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local, content_type="text/csv", timeout=300)
    blob.reload()
    return {
        "name": blob.name,
        "size": int(blob.size or 0),
        "updated": blob.updated.isoformat() if blob.updated else "",
        "generation": str(blob.generation or ""),
        "method": "google.cloud.storage",
    }


def _upload_with_gcloud(local: str, uri: str, project: str) -> Dict[str, Any]:
    cp = _gcloud_cmd(
        ["gcloud", "storage", "cp", local, uri, f"--project={project}"]
    )
    if cp.returncode != 0:
        raise RuntimeError(
            (cp.stderr or cp.stdout or f"gcloud storage cp failed ({cp.returncode})").strip()
        )
    desc = _gcloud_cmd(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            uri,
            f"--project={project}",
            "--format=json",
        ]
    )
    if desc.returncode != 0:
        raise RuntimeError(
            (desc.stderr or desc.stdout or "gcloud storage objects describe failed").strip()
        )
    info = json.loads(desc.stdout or "{}")
    size = info.get("size")
    if size is None:
        size = (info.get("metadata") or {}).get("size")
    return {
        "name": info.get("name") or uri,
        "size": int(size or 0),
        "updated": str(info.get("updated") or info.get("timeCreated") or ""),
        "generation": str(info.get("generation") or ""),
        "method": "gcloud storage cp",
    }


def upload_run_csv(
    *,
    pipeline_id: str,
    research_date: str,
    csv_path: str,
    bucket: str = DEFAULT_BUCKET,
    project: str = DEFAULT_PROJECT,
) -> Dict[str, Any]:
    pid = canonicalize_pipeline_id(pipeline_id)
    date = validate_research_date(research_date)
    rows = validate_local_csv(csv_path)
    uri = gcs_object_uri(pid, date, bucket=bucket)
    project = (project or DEFAULT_PROJECT).strip()
    try:
        meta = _upload_with_storage_client(csv_path, uri, project)
    except Exception as storage_error:
        try:
            meta = _upload_with_gcloud(csv_path, uri, project)
        except FileNotFoundError as gcloud_missing:
            raise RuntimeError(
                f"GCS upload failed ({type(storage_error).__name__}: {storage_error})"
            ) from storage_error
        except Exception as gcloud_error:
            raise RuntimeError(
                f"GCS upload failed via client ({storage_error}) and gcloud ({gcloud_error})"
            ) from gcloud_error
    size = int(meta.get("size") or 0)
    if size <= 0:
        raise RuntimeError(f"Uploaded object is empty: {uri}")
    local_size = os.path.getsize(csv_path)
    if size != local_size:
        raise RuntimeError(
            f"GCS size {size} != local size {local_size} for {uri}"
        )
    return {
        "ok": True,
        "pipeline_id": pid,
        "research_date": date,
        "local_path": os.path.abspath(csv_path),
        "gcs_uri": uri,
        "rows_excluding_header": rows,
        "bytes": size,
        "updated": meta.get("updated") or "",
        "generation": meta.get("generation") or "",
        "method": meta.get("method") or "",
        "replaced_same_object": True,
    }


def upload_run_csv_after_success(
    *,
    run_fn,
    pipeline_id: str,
    research_date: str,
    cfg,
    pipeline,
    csv_paths: Iterable[str],
    skip: bool,
) -> int:
    """Call only after collect + enrich + validate already succeeded."""
    if skip:
        print("Skipping GCS archive (--skip-gcs)", flush=True)
        return 0
    try:
        local = resolve_run_csv_file(cfg, pipeline, research_date, list(csv_paths or []))
    except (FileNotFoundError, ValueError) as e:
        print(f"GCS archive failed: {e}", flush=True)
        return 2
    return int(
        run_fn(
            "common/scripts/upload_run_csv.py",
            [
                "--pipeline",
                pipeline_id,
                "--date",
                research_date,
                "--file",
                local,
            ],
        )
    )
