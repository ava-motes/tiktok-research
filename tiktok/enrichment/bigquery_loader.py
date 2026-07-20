"""BigQuery sync — simplified production architecture.

ONLY two BigQuery tables:
  - tiktok_video_enriched  (analytics, one row per video)
  - tiktok_pipeline_logs   (ops/debug events)

Legacy BQ tables (videos_raw, video_transcripts, video_ocr, video_emojis)
are deprecated and must not be written.

SQLite on comm-cme-p01 remains temporary staging only.
TikTok Research API collection is unchanged.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from tiktok.enrichment.emoji_extract import aggregate_emoji_fields
from tiktok.enrichment.ocr_postprocess import aggregate_ocr_outputs

logger = logging.getLogger(__name__)

DEFAULT_GCP_PROJECT = "cfme-mediaengagment-prod"
DEFAULT_BQ_DATASET = "tiktok_research"

ENRICHED_TABLE = "tiktok_video_enriched"
PIPELINE_LOGS_TABLE = "tiktok_pipeline_logs"
PIPELINE_VERSION = "enrichment-v4.1"

# Deprecated — never write these from enrichment
LEGACY_BQ_TABLES = (
    "videos_raw",
    "video_transcripts",
    "video_ocr",
    "video_emojis",
)

VISION_USD_PER_IMAGE = 1.50 / 1000.0
WHISPER_USD_PER_MINUTE = 0.006

# Final analytics schema — content only (ops live in tiktok_pipeline_logs)
BQ_SCHEMAS: Dict[str, List[Dict[str, str]]] = {
    ENRICHED_TABLE: [
        # Video metadata
        {"name": "video_id", "type": "STRING"},
        {"name": "video_url", "type": "STRING"},
        {"name": "creator_username", "type": "STRING"},
        {"name": "creator_display_name", "type": "STRING"},
        {"name": "creator_bio", "type": "STRING"},
        {"name": "creator_verified", "type": "BOOLEAN"},
        {"name": "creator_followers", "type": "INTEGER"},
        {"name": "creator_following", "type": "INTEGER"},
        {"name": "creator_total_likes", "type": "INTEGER"},
        {"name": "creator_video_count", "type": "INTEGER"},
        {"name": "posted_at", "type": "STRING"},
        {"name": "caption", "type": "STRING"},
        {"name": "hashtags", "type": "STRING"},
        {"name": "like_count", "type": "INTEGER"},
        {"name": "comment_count", "type": "INTEGER"},
        {"name": "share_count", "type": "INTEGER"},
        {"name": "favorite_count", "type": "INTEGER"},
        {"name": "video_duration_seconds", "type": "FLOAT"},
        {"name": "voice_to_text", "type": "STRING"},
        {"name": "sticker_text", "type": "STRING"},
        # Optional comments: JSON array of
        # {comment_id, video_id, comment_text, comment_likes, comment_timestamp, parent_comment_id}
        {"name": "comments_json", "type": "STRING"},
        # Enrichment — Whisper
        {"name": "whisper_transcript", "type": "STRING"},
        {"name": "whisper_status", "type": "STRING"},
        {"name": "whisper_latency_seconds", "type": "FLOAT"},
        # Enrichment — OCR (ocr_text = cleaned; raw preserved separately)
        {"name": "ocr_text", "type": "STRING"},
        {"name": "raw_ocr_text", "type": "STRING"},
        {"name": "cleaned_ocr_text", "type": "STRING"},
        {"name": "ocr_quality_score", "type": "INTEGER"},
        {"name": "ocr_character_count", "type": "INTEGER"},
        {"name": "ocr_unique_text_ratio", "type": "FLOAT"},
        {"name": "ocr_source_count", "type": "INTEGER"},
        # Enrichment — emoji
        {"name": "emoji_characters", "type": "STRING"},
        {"name": "emoji_descriptions", "type": "STRING"},
        {"name": "emoji_category", "type": "STRING"},
        {"name": "emoji_source", "type": "STRING"},
        # Light status (detailed ops → tiktok_pipeline_logs)
        {"name": "enrichment_status", "type": "STRING"},
        {"name": "enrichment_quality_score", "type": "INTEGER"},
        {"name": "failure_reason", "type": "STRING"},
        {"name": "enrichment_date", "type": "STRING"},
        {"name": "pipeline_version", "type": "STRING"},
    ],
    PIPELINE_LOGS_TABLE: [
        {"name": "log_id", "type": "STRING"},
        {"name": "video_id", "type": "STRING"},
        {"name": "stage", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "retry_count", "type": "INTEGER"},
        {"name": "pipeline_version", "type": "STRING"},
        {"name": "start_time", "type": "STRING"},
        {"name": "end_time", "type": "STRING"},
        {"name": "duration_seconds", "type": "FLOAT"},
        {"name": "error_type", "type": "STRING"},
        {"name": "error_message", "type": "STRING"},
        {"name": "worker_hostname", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
}


def enrichment_quality_score(
    *,
    has_metadata: bool,
    has_ocr: bool = False,
    has_transcript: bool = False,
    has_emoji: bool = False,
    has_voice_to_text: bool = False,
    has_sticker: bool = False,
    # Back-compat aliases
    has_whisper: Optional[bool] = None,
) -> int:
    """Modality-based score (0–100). Emoji/sticker are optional bonuses.

    Metadata 20 · Whisper 30 · OCR 30 · voice_to_text 10 · emoji 5 · sticker 5
    """
    if not has_metadata:
        return 0
    whisper = has_transcript if has_whisper is None else bool(has_whisper)
    score = 20
    if whisper:
        score += 30
    if has_ocr:
        score += 30
    if has_voice_to_text:
        score += 10
    if has_emoji:
        score += 5
    if has_sticker:
        score += 5
    return min(100, score)


def gcp_project() -> str:
    return (
        os.environ.get("BIGQUERY_PROJECT", "").strip()
        or os.environ.get("GCP_PROJECT", "").strip()
        or DEFAULT_GCP_PROJECT
    )


def bq_dataset() -> str:
    return os.environ.get("BIGQUERY_DATASET", "").strip() or DEFAULT_BQ_DATASET


def vision_enabled() -> bool:
    raw = os.environ.get("VISION_ENABLED", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


def bigquery_configured() -> bool:
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    return bool(gcp_project() and bq_dataset() and creds and os.path.isfile(creds))


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=gcp_project())


def enriched_table_id() -> str:
    return f"{gcp_project()}.{bq_dataset()}.{ENRICHED_TABLE}"


def pipeline_logs_table_id() -> str:
    return f"{gcp_project()}.{bq_dataset()}.{PIPELINE_LOGS_TABLE}"


def _ensure_table(client, table_key: str) -> None:
    from google.cloud import bigquery

    fields = BQ_SCHEMAS[table_key]
    table_id = f"{gcp_project()}.{bq_dataset()}.{table_key}"
    schema = [bigquery.SchemaField(f["name"], f["type"]) for f in fields]
    client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)

    ddl_type = {
        "STRING": "STRING",
        "INTEGER": "INT64",
        "FLOAT": "FLOAT64",
        "BOOLEAN": "BOOL",
        "TIMESTAMP": "TIMESTAMP",
    }
    existing = {f.name for f in client.get_table(table_id).schema}
    for f in fields:
        if f["name"] in existing:
            continue
        bq_type = ddl_type.get(f["type"], f["type"])
        client.query(
            f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS {f['name']} {bq_type}"
        ).result()
        logger.info("Added BigQuery column %s.%s (%s)", table_key, f["name"], bq_type)


def ensure_dataset_and_tables() -> None:
    """Create dataset + the two production tables; never create legacy tables."""
    if not bigquery_configured():
        raise RuntimeError(
            "BigQuery not configured on server. Set on comm-cme-p01:\n"
            "  GOOGLE_APPLICATION_CREDENTIALS=...\n"
            "  GCP_PROJECT=cfme-mediaengagment-prod\n"
            "  BIGQUERY_DATASET=tiktok_research"
        )
    from google.cloud import bigquery

    client = _client()
    ds_ref = bigquery.Dataset(f"{gcp_project()}.{bq_dataset()}")
    ds_ref.location = os.environ.get("BIGQUERY_LOCATION", "US")
    client.create_dataset(ds_ref, exists_ok=True)

    _ensure_table(client, ENRICHED_TABLE)
    _ensure_table(client, PIPELINE_LOGS_TABLE)
    logger.info(
        "BigQuery ready: %s + %s (legacy tables not created)",
        enriched_table_id(),
        pipeline_logs_table_id(),
    )


def _latest_worker_latency(conn, video_id: str, worker: str) -> Optional[float]:
    row = conn.execute(
        """SELECT elapsed_seconds FROM enrichment_log
           WHERE video_id=? AND worker=?
           ORDER BY id DESC LIMIT 1""",
        (video_id, worker),
    ).fetchone()
    if not row:
        return None
    try:
        return float(row[0] if not hasattr(row, "keys") else row["elapsed_seconds"])
    except (TypeError, ValueError):
        return None


def _latest_worker_error(conn, video_id: str, worker: str) -> Optional[str]:
    row = conn.execute(
        """SELECT error, ok FROM enrichment_log
           WHERE video_id=? AND worker=?
           ORDER BY id DESC LIMIT 1""",
        (video_id, worker),
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    if int(d.get("ok") or 0) == 1:
        return None
    return (d.get("error") or "").strip() or None


def _enrichment_status(
    *,
    audio_available: bool,
    transcript_chars: int,
    ocr_frames: int,
    transcript_error: Optional[str],
    ocr_error: Optional[str],
) -> Tuple[str, str]:
    reasons = []
    if transcript_error:
        reasons.append(f"whisper:{transcript_error}")
    if ocr_error:
        reasons.append(f"ocr:{ocr_error}")

    has_ocr = ocr_frames > 0
    has_speech = transcript_chars > 0
    whisper_ok = audio_available and not transcript_error

    if has_ocr and (has_speech or whisper_ok):
        status = "ok" if not reasons else "partial"
    elif has_ocr or has_speech or whisper_ok:
        status = "partial"
    elif reasons:
        status = "failed"
    else:
        status = "partial"
    return status, " | ".join(reasons)


def _comments_json(conn, video_id: str) -> str:
    try:
        rows = conn.execute(
            """SELECT comment_id, parent_comment_id, text, like_count, posted_at, create_time
               FROM comments WHERE video_id=? ORDER BY create_time""",
            (video_id,),
        ).fetchall()
    except Exception:
        return "[]"
    out = []
    for r in rows:
        d = dict(r)
        out.append(
            {
                "comment_id": d.get("comment_id") or "",
                "video_id": video_id,
                "comment_text": d.get("text") or "",
                "comment_likes": d.get("like_count"),
                "comment_timestamp": d.get("posted_at")
                or (str(d.get("create_time") or "")),
                "parent_comment_id": d.get("parent_comment_id") or "",
            }
        )
    return json.dumps(out, ensure_ascii=False)


def build_enriched_row(conn, video_id: str) -> Optional[Dict[str, Any]]:
    """Aggregate SQLite staging into one BigQuery analytics row (final schema)."""
    v = conn.execute(
        """SELECT video_id, username, caption, create_time, posted_at,
                  like_count, comment_count, share_count, save_count, hashtags, video_url,
                  duration_seconds, inserted_at, voice_to_text, sticker_overlay_text
           FROM videos WHERE video_id=?""",
        (video_id,),
    ).fetchone()
    if not v:
        return None
    v = dict(v)

    user = {}
    uname = v.get("username") or ""
    if uname:
        u = conn.execute(
            """SELECT display_name, bio, is_verified, follower_count, following_count,
                      likes_count, video_count FROM users WHERE username=?""",
            (uname,),
        ).fetchone()
        user = dict(u) if u else {}

    t = conn.execute(
        """SELECT transcript, language, whisper_model, status, error,
                  audio_duration_seconds
           FROM video_transcripts WHERE video_id=?""",
        (video_id,),
    ).fetchone()
    t = dict(t) if t else {}

    ocr = [
        dict(r)
        for r in conn.execute(
            """SELECT ocr_text, ocr_text_raw, ocr_text_clean, confidence, source,
                      source_type, frame_timestamp, frame_number, frame_pct
               FROM video_ocr
               WHERE video_id=? ORDER BY frame_timestamp, frame_number""",
            (video_id,),
        ).fetchall()
    ]
    em = [
        dict(r)
        for r in conn.execute(
            """SELECT emoji, emoji_name, emoji_category, count, text_source,
                      emoji_codepoint, emoji_kind
               FROM video_emojis WHERE video_id=? ORDER BY id""",
            (video_id,),
        ).fetchall()
    ]

    audio_dur = t.get("audio_duration_seconds")
    try:
        audio_dur_f = float(audio_dur) if audio_dur is not None else None
    except (TypeError, ValueError):
        audio_dur_f = None
    duration_s = float(v.get("duration_seconds") or 0) or float(audio_dur_f or 0)
    ocr_agg = aggregate_ocr_outputs(ocr, duration_seconds=duration_s or None)
    emoji_agg = aggregate_emoji_fields(em)

    status = (t.get("status") or "").lower()
    audio_available = status == "ok"
    transcript = (t.get("transcript") or "") if audio_available else ""
    # Prefer cleaned OCR for analytics; keep full raw separately
    cleaned_ocr = (ocr_agg.get("cleaned_ocr_text") or ocr_agg.get("ocr_text") or "").strip()
    raw_ocr = (ocr_agg.get("raw_ocr_text") or "").strip()
    ocr_text = cleaned_ocr
    frames_text = int(ocr_agg.get("frames_with_text") or 0)
    # Meaningful OCR for status: cleaned text or quality above garbage floor
    ocr_quality = int(ocr_agg.get("ocr_quality_score") or 0)
    has_meaningful_ocr = bool(cleaned_ocr) and ocr_quality >= 25

    ocr_error = _latest_worker_error(conn, video_id, "ocr")
    # Ignore stale OCR errors once meaningful cleaned OCR is present
    if has_meaningful_ocr:
        ocr_error = None
    tr_error = t.get("error") or _latest_worker_error(conn, video_id, "transcription")
    whisper_latency = _latest_worker_latency(conn, video_id, "transcription")
    vtt = (v.get("voice_to_text") or "").strip()
    sticker = (v.get("sticker_overlay_text") or "").strip()
    enrich_status, failure_reason = _enrichment_status(
        audio_available=audio_available,
        transcript_chars=len(transcript),
        ocr_frames=frames_text if has_meaningful_ocr else 0,
        transcript_error=tr_error if status != "ok" else None,
        ocr_error=ocr_error,
    )
    quality = enrichment_quality_score(
        has_metadata=bool(v.get("video_id") and uname),
        has_whisper=bool(transcript),
        has_ocr=has_meaningful_ocr,
        has_voice_to_text=bool(vtt),
        has_emoji=bool(emoji_agg.get("emoji_characters")),
        has_sticker=bool(sticker),
    )
    # Speech-only / no-overlay videos are OK when Whisper (or VTT) succeeded
    # and there is no active worker failure. Missing emoji must not force partial.
    if enrich_status == "partial" and not failure_reason and (transcript or vtt):
        if not ocr_error and (status == "ok" or bool(vtt)):
            enrich_status = "ok"
    if enrich_status == "ok" and not transcript and not vtt and not has_meaningful_ocr:
        enrich_status = "partial"
        failure_reason = failure_reason or "missing:text_layers"

    posted_at = v.get("posted_at") or (
        str(v.get("create_time") or "") if v.get("create_time") else ""
    )
    now = datetime.now(timezone.utc)
    if transcript:
        whisper_status = "ok"
    elif status == "error":
        whisper_status = "error"
    elif status:
        whisper_status = status
    else:
        whisper_status = "missing"

    return {
        "video_id": v["video_id"],
        "video_url": v.get("video_url") or "",
        "creator_username": uname,
        "creator_display_name": user.get("display_name") or "",
        "creator_bio": user.get("bio") or "",
        "creator_verified": bool(user.get("is_verified")),
        "creator_followers": user.get("follower_count"),
        "creator_following": user.get("following_count"),
        "creator_total_likes": user.get("likes_count"),
        "creator_video_count": user.get("video_count"),
        "posted_at": posted_at,
        "caption": v.get("caption") or "",
        "hashtags": v.get("hashtags") or "",
        "like_count": v.get("like_count"),
        "comment_count": v.get("comment_count"),
        "share_count": v.get("share_count"),
        "favorite_count": v.get("save_count"),
        "video_duration_seconds": duration_s or None,
        "voice_to_text": vtt,
        "sticker_text": v.get("sticker_overlay_text") or "",
        "comments_json": _comments_json(conn, video_id),
        "whisper_transcript": transcript,
        "whisper_status": whisper_status,
        "whisper_latency_seconds": whisper_latency,
        "ocr_text": ocr_text,
        "raw_ocr_text": raw_ocr,
        "cleaned_ocr_text": cleaned_ocr,
        "ocr_quality_score": ocr_quality,
        "ocr_character_count": int(ocr_agg.get("ocr_character_count") or len(cleaned_ocr)),
        "ocr_unique_text_ratio": ocr_agg.get("ocr_unique_text_ratio"),
        "ocr_source_count": int(ocr_agg.get("ocr_source_count") or 0),
        "emoji_characters": emoji_agg.get("emoji_characters") or "",
        "emoji_descriptions": emoji_agg.get("emoji_descriptions") or "",
        "emoji_category": emoji_agg.get("emoji_category") or "",
        "emoji_source": emoji_agg.get("emoji_source") or "",
        "enrichment_status": enrich_status,
        "enrichment_quality_score": quality,
        "failure_reason": failure_reason or "",
        "enrichment_date": now.date().isoformat(),
        "pipeline_version": PIPELINE_VERSION,
    }


def load_enriched_rows(
    rows: List[Dict[str, Any]],
    *,
    write_disposition: str = "WRITE_APPEND",
) -> int:
    if not rows:
        return 0
    if not bigquery_configured():
        raise RuntimeError("BigQuery not configured")
    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=[
            bigquery.SchemaField(f["name"], f["type"]) for f in BQ_SCHEMAS[ENRICHED_TABLE]
        ],
    )
    job = client.load_table_from_json(rows, enriched_table_id(), job_config=job_config)
    job.result()
    logger.info("Loaded %s rows into %s", len(rows), enriched_table_id())
    return len(rows)


def append_pipeline_logs(rows: List[Dict[str, Any]]) -> int:
    """Append operational events to tiktok_pipeline_logs (never to analytics)."""
    if not rows:
        return 0
    ensure_dataset_and_tables()
    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField(f["name"], f["type"])
            for f in BQ_SCHEMAS[PIPELINE_LOGS_TABLE]
        ],
    )
    job = client.load_table_from_json(
        rows, pipeline_logs_table_id(), job_config=job_config
    )
    job.result()
    return len(rows)


def pipeline_logs_from_sqlite(conn, video_id: str) -> List[Dict[str, Any]]:
    """Convert local enrichment_log rows for a video into BQ pipeline log events."""
    hostname = socket.gethostname()
    now = datetime.now(timezone.utc).isoformat()
    out: List[Dict[str, Any]] = []
    for r in conn.execute(
        """SELECT worker, ok, started_at, ended_at, elapsed_seconds, error, detail_json
           FROM enrichment_log WHERE video_id=? ORDER BY id""",
        (video_id,),
    ).fetchall():
        d = dict(r)
        err = (d.get("error") or "").strip()
        out.append(
            {
                "log_id": str(uuid.uuid4()),
                "video_id": video_id,
                "stage": d.get("worker") or "unknown",
                "status": "ok" if int(d.get("ok") or 0) == 1 else "error",
                "retry_count": 0,
                "pipeline_version": PIPELINE_VERSION,
                "start_time": d.get("started_at") or "",
                "end_time": d.get("ended_at") or "",
                "duration_seconds": d.get("elapsed_seconds"),
                "error_type": (err.split(":")[0][:80] if err else ""),
                "error_message": err[:500],
                "worker_hostname": hostname,
                "created_at": now,
            }
        )
    # Always add a bq_sync event when called from sync
    out.append(
        {
            "log_id": str(uuid.uuid4()),
            "video_id": video_id,
            "stage": "bq_sync",
            "status": "ok",
            "retry_count": 0,
            "pipeline_version": PIPELINE_VERSION,
            "start_time": now,
            "end_time": now,
            "duration_seconds": 0.0,
            "error_type": "",
            "error_message": "",
            "worker_hostname": hostname,
            "created_at": now,
        }
    )
    return out


def sync_video_from_sqlite(conn, video_id: str) -> Dict[str, int]:
    """Upsert one enriched analytics row + append pipeline logs."""
    ensure_dataset_and_tables()
    row = build_enriched_row(conn, video_id)
    if not row:
        logger.warning("No videos row for %s; skip BQ sync", video_id)
        return {ENRICHED_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from tiktok.enrichment.validate_row import validate_enriched_row

    ok, errors = validate_enriched_row(row)
    if not ok:
        logger.error("Validation failed for %s: %s — skip BQ upload", video_id, errors)
        return {ENRICHED_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from google.cloud import bigquery

    from tiktok.enrichment.store import touch_pipeline_status

    client = _client()
    table_id = enriched_table_id()
    client.query(
        f"DELETE FROM `{table_id}` WHERE video_id = @vid",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vid", "STRING", video_id)
            ]
        ),
    ).result()

    n = load_enriched_rows([row])
    log_rows = pipeline_logs_from_sqlite(conn, video_id)
    n_logs = 0
    try:
        n_logs = append_pipeline_logs(log_rows)
    except Exception as e:
        logger.warning("Pipeline logs upload failed for %s: %s", video_id, e)

    if n > 0:
        uploaded_at = datetime.now(timezone.utc).isoformat()
        touch_pipeline_status(conn, video_id, bq_uploaded=uploaded_at)
        conn.commit()

    return {ENRICHED_TABLE: n, PIPELINE_LOGS_TABLE: n_logs}


def count_enriched_rows(video_ids: Optional[List[str]] = None) -> int:
    ensure_dataset_and_tables()
    from google.cloud import bigquery

    client = _client()
    table_id = enriched_table_id()
    if video_ids:
        params = [bigquery.ArrayQueryParameter("vids", "STRING", video_ids)]
        q = f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE video_id IN UNNEST(@vids)"
        job = client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params))
    else:
        job = client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`")
    rows = list(job.result())
    return int(rows[0].n) if rows else 0


def inspect_schema() -> Dict[str, Any]:
    ensure_dataset_and_tables()
    client = _client()
    out: Dict[str, Any] = {"tables": {}}
    for key, table_id in (
        (ENRICHED_TABLE, enriched_table_id()),
        (PIPELINE_LOGS_TABLE, pipeline_logs_table_id()),
    ):
        table = client.get_table(table_id)
        names = [f.name for f in table.schema]
        expected = [f["name"] for f in BQ_SCHEMAS[key]]
        out["tables"][key] = {
            "table": table_id,
            "fields": names,
            "missing": [f for f in expected if f not in names],
            "extra": [f for f in names if f not in expected],
        }
    # Legacy presence check
    legacy = {}
    for name in LEGACY_BQ_TABLES:
        tid = f"{gcp_project()}.{bq_dataset()}.{name}"
        try:
            client.get_table(tid)
            legacy[name] = "exists_deprecated"
        except Exception:
            legacy[name] = "absent"
    out["legacy_tables"] = legacy
    return out
