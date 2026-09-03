"""Upload one collection-date CSV to the shared UT Box pipeline folders.

After BigQuery is updated, each pipeline run writes ``YYYY-MM-DD.csv`` into
the matching subfolder of the shared Box folder.

Credentials (server ``.env``): OAuth ``BOX_CLIENT_ID`` / ``BOX_CLIENT_SECRET``
plus ``data/box_oauth.json``, or Box CCG with ``BOX_ENTERPRISE_ID``, or a
short-lived ``BOX_ACCESS_TOKEN``. If none are set, delivery is skipped.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import requests

logger = logging.getLogger(__name__)

BOX_TOKEN_URL = "https://api.box.com/oauth2/token"
BOX_API = "https://api.box.com/2.0"
BOX_UPLOAD = "https://upload.box.com/api/2.0"
DEFAULT_SHARED_LINK = "https://utexas.box.com/s/drgh994ljt4q9c8asvizbg9mmkosif41"

PIPELINE_FOLDER_ALIASES = {
    "content_creators": (
        "p1_content_creators",
        "content_creators",
        "content creators",
        "pipeline 1",
        "p1",
        "pipeline1",
        "creators",
    ),
    "news": (
        "p2_news",
        "news",
        "news_accounts",
        "news accounts",
        "pipeline 2",
        "p2",
        "pipeline2",
    ),
    "keyword": (
        "p3_key_words",
        "p3_keywords",
        "keyword",
        "keywords",
        "keyword_search",
        "pipeline 3",
        "p3",
        "pipeline3",
    ),
}

PIPELINE_BQ_TABLE = {
    "content_creators": "content_creators",
    "news": "news",
    "keyword": "keyword",
}


def match_pipeline_folder(
    pipeline_id: str,
    folder_names: Sequence[str],
    configured: Optional[str] = None,
) -> str:
    """Pick the Box subfolder name for a pipeline id."""
    names = [n for n in folder_names if (n or "").strip()]
    if configured and configured.strip():
        want = configured.strip().lower()
        for n in names:
            if n.strip().lower() == want:
                return n
        raise RuntimeError(
            f"Box folder {configured!r} not found for pipeline {pipeline_id}. "
            f"Available: {names}"
        )
    aliases = PIPELINE_FOLDER_ALIASES.get(pipeline_id) or ()
    hits: List[str] = []
    for n in names:
        key = n.strip().lower().replace("-", " ").replace("_", " ")
        key = " ".join(key.split())
        for alias in aliases:
            an = alias.replace("_", " ")
            if key == an or an in key:
                hits.append(n)
                break
    hits = list(dict.fromkeys(hits))
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        raise RuntimeError(
            f"Ambiguous Box folders for {pipeline_id}: {hits}. "
            "Set box.pipeline_folders in config.yaml."
        )
    raise RuntimeError(
        f"No Box subfolder matched pipeline {pipeline_id}. "
        f"Looked for {list(aliases)}. Available: {names}. "
        "Set box.pipeline_folders in config.yaml to the exact folder names."
    )


def _oauth_store_path() -> str:
    return (os.environ.get("BOX_OAUTH_STORE") or "").strip() or os.path.join(
        "data", "box_oauth.json"
    )


def _read_oauth_store() -> Dict[str, Any]:
    path = _oauth_store_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.warning("Could not read Box OAuth store %s", path)
        return {}


def _write_oauth_store(data: Dict[str, Any]) -> None:
    path = _oauth_store_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    payload = dict(data)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _refresh_oauth_token(
    client_id: str, client_secret: str, refresh_token: str
) -> Optional[str]:
    resp = requests.post(
        BOX_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json() or {}
    access = body.get("access_token")
    new_refresh = (body.get("refresh_token") or "").strip()
    if new_refresh:
        store = _read_oauth_store()
        store["refresh_token"] = new_refresh
        _write_oauth_store(store)
    return str(access) if access else None


def _ccg_token(client_id: str, client_secret: str) -> Optional[str]:
    user_id = (os.environ.get("BOX_USER_ID") or "").strip()
    enterprise = (os.environ.get("BOX_ENTERPRISE_ID") or "").strip()
    if user_id:
        subject_type, subject_id = "user", user_id
    elif enterprise:
        subject_type, subject_id = "enterprise", enterprise
    else:
        return None
    resp = requests.post(
        BOX_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "box_subject_type": subject_type,
            "box_subject_id": subject_id,
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = (resp.json() or {}).get("access_token")
    return str(token) if token else None


def _box_token() -> Optional[str]:
    direct = (os.environ.get("BOX_ACCESS_TOKEN") or "").strip()
    if direct:
        return direct
    client_id = (os.environ.get("BOX_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("BOX_CLIENT_SECRET") or "").strip()
    if not (client_id and client_secret):
        return None
    refresh = (os.environ.get("BOX_REFRESH_TOKEN") or "").strip()
    store = _read_oauth_store()
    refresh = (store.get("refresh_token") or "").strip() or refresh
    if refresh:
        return _refresh_oauth_token(client_id, client_secret, refresh)
    return _ccg_token(client_id, client_secret)


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


def box_configured() -> bool:
    return bool(_box_token())


def _headers(token: str, shared_link: Optional[str] = None) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {token}"}
    if shared_link:
        h["BoxApi"] = f"shared_link={shared_link}"
    return h


def _shared_item(token: str, shared_link: str) -> Dict[str, Any]:
    resp = requests.get(
        f"{BOX_API}/shared_items",
        headers=_headers(token, shared_link),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _folder_items(token: str, folder_id: str, shared_link: Optional[str] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    offset = 0
    while True:
        resp = requests.get(
            f"{BOX_API}/folders/{folder_id}/items",
            headers=_headers(token, shared_link),
            params={"limit": 1000, "offset": offset, "fields": "id,name,type"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json() or {}
        batch = list(data.get("entries") or [])
        items.extend(batch)
        total = int(data.get("total_count") or 0)
        offset += len(batch)
        if not batch or offset >= total:
            break
    return items


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "|".join(_csv_cell(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# Leftmost CSV columns so failed-handle rows are obvious when the file opens.
CSV_LEAD_FIELDS = (
    "collection_status",
    "api_error_code",
    "failure_reason",
    "creator_username",
    "video_id",
    "collection_date",
)


def csv_export_fields(schema_fields: Sequence[str]) -> List[str]:
    """Schema columns with status / handle / video_id first for review."""
    names = [str(name) for name in schema_fields]
    lead = [name for name in CSV_LEAD_FIELDS if name in names]
    rest = [name for name in names if name not in set(lead)]
    return lead + rest


def collection_export_sql(project: str, dataset: str, table: str, fields: Sequence[str]) -> str:
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


def _upload_file(
    token: str,
    folder_id: str,
    local_path: str,
    filename: str,
    shared_link: Optional[str] = None,
    existing_file_id: Optional[str] = None,
) -> Dict[str, Any]:
    headers = _headers(token, shared_link)
    with open(local_path, "rb") as fh:
        if existing_file_id:
            resp = requests.post(
                f"{BOX_UPLOAD}/files/{existing_file_id}/content",
                headers=headers,
                files={"file": (filename, fh)},
                timeout=300,
            )
        else:
            attrs = json.dumps({"name": filename, "parent": {"id": folder_id}})
            resp = requests.post(
                f"{BOX_UPLOAD}/files/content",
                headers=headers,
                data={"attributes": attrs},
                files={"file": (filename, fh)},
                timeout=300,
            )
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def deliver_pipeline_csv_to_box(
    *,
    pipeline_id: str,
    collection_date: str,
    box_cfg: Optional[Dict[str, Any]] = None,
    csv_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Export BQ CSV for the date and upload as ``{collection_date}.csv``."""
    if pipeline_id not in PIPELINE_BQ_TABLE:
        raise ValueError(f"Unknown pipeline {pipeline_id}")
    token = _box_token()
    if not token:
        logger.warning("Box credentials not set — skip CSV upload")
        return {"skipped": True, "reason": "missing_box_credentials"}

    cfg = box_cfg or {}
    shared_link = (
        (cfg.get("shared_link") or "").strip()
        or (os.environ.get("BOX_SHARED_LINK") or "").strip()
        or DEFAULT_SHARED_LINK
    )
    filename = f"{collection_date}.csv"
    if csv_path:
        local = csv_path
        if not os.path.isfile(local):
            raise FileNotFoundError(f"CSV not found: {local}")
        with open(local, newline="", encoding="utf-8-sig") as f:
            rows = max(sum(1 for _ in csv.reader(f)) - 1, 0)
    else:
        local_dirs = cfg.get("pipeline_local_dirs") or {}
        local_root = str((local_dirs.get(pipeline_id) if isinstance(local_dirs, dict) else "") or "").strip()
        if not local_root:
            defaults = {
                "content_creators": "p1_content_creators/box",
                "news": "p2_news/box",
                "keyword": "p3_keywords/box",
            }
            local_root = defaults.get(pipeline_id) or os.path.join("data", "exports", pipeline_id)
        local = os.path.join(local_root, filename)
        rows = export_bq_collection_csv(pipeline_id, collection_date, local)

    ids_cfg = cfg.get("pipeline_folder_ids") or {}
    folders_cfg = cfg.get("pipeline_folders") or {}
    configured_name = (
        folders_cfg.get(pipeline_id) if isinstance(folders_cfg, dict) else None
    )
    folder_id = str((ids_cfg.get(pipeline_id) if isinstance(ids_cfg, dict) else "") or "").strip()
    folder_name = str(configured_name or "").strip() or pipeline_id
    link_for_api: Optional[str] = None

    if not folder_id:
        link_for_api = shared_link
        parent = _shared_item(token, shared_link)
        parent_id = str((parent.get("id") or ""))
        if (parent.get("type") or "") != "folder" or not parent_id:
            raise RuntimeError(f"Box shared link is not a folder: {parent}")
        children = _folder_items(token, parent_id, shared_link)
        folder_names = [c["name"] for c in children if c.get("type") == "folder"]
        folder_name = match_pipeline_folder(pipeline_id, folder_names, configured_name)
        folder = next(
            c
            for c in children
            if c.get("type") == "folder" and c.get("name") == folder_name
        )
        folder_id = str(folder["id"])

    dest_items = _folder_items(token, folder_id, link_for_api)
    existing = next(
        (
            c
            for c in dest_items
            if c.get("type") == "file" and c.get("name") == filename
        ),
        None,
    )
    uploaded = _upload_file(
        token,
        folder_id,
        local,
        filename,
        shared_link=link_for_api,
        existing_file_id=str(existing["id"]) if existing else None,
    )
    file_id = ""
    entries = uploaded.get("entries") if isinstance(uploaded, dict) else None
    if entries:
        file_id = str((entries[0] or {}).get("id") or "")
    logger.info(
        "Uploaded Box %s/%s (%s rows) file_id=%s",
        folder_name,
        filename,
        rows,
        file_id,
    )
    return {
        "skipped": False,
        "pipeline_id": pipeline_id,
        "collection_date": collection_date,
        "folder": folder_name,
        "folder_id": folder_id,
        "filename": filename,
        "rows": rows,
        "local_path": local,
        "box_file_id": file_id,
        "replaced": bool(existing),
        "shared_link": shared_link,
    }


def maybe_deliver_after_bq(
    *,
    pipeline_id: str,
    collection_date: str,
    box_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Best-effort Box delivery. Never raises into the caller."""
    if not pipeline_id or not collection_date:
        return {"skipped": True, "reason": "missing_pipeline_or_date"}
    try:
        return deliver_pipeline_csv_to_box(
            pipeline_id=pipeline_id,
            collection_date=collection_date,
            box_cfg=box_cfg,
        )
    except Exception as e:
        logger.error("Box CSV upload failed: %s", e)
        return {"skipped": False, "ok": False, "error": str(e)}
