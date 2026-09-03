"""BigQuery sync — v5.0 production path plus additive collection tables.

v5.0 tables (unchanged default):
  - tiktok_video_enriched  (analytics, one row per video)
  - tiktok_pipeline_logs   (ops/debug events)

Isolated collection tables (created only by their ensure_* helpers):
  - content_creators  (Pipeline 1)
  - news              (Pipeline 2)
  - keyword           (Pipeline 3)

Do not write those rows to tiktok_video_enriched. Do not DROP the v5.0 table.
Older unused table names (tiktok_content_creators / tiktok_news_accounts /
tiktok_keyword_search) are not written by this loader.


Legacy BQ tables (videos_raw, video_transcripts, video_ocr, video_emojis)
are deprecated and must not be written.

SQLite on comm-cme-p01 remains temporary staging only.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from enrichment.emoji_extract import aggregate_emoji_fields
from enrichment.ocr_postprocess import aggregate_ocr_outputs

logger = logging.getLogger(__name__)

DEFAULT_GCP_PROJECT = "cfme-mediaengagment-prod"
DEFAULT_BQ_DATASET = "tiktok_research"

ENRICHED_TABLE = "tiktok_video_enriched"
PIPELINE_LOGS_TABLE = "tiktok_pipeline_logs"
CONTENT_CREATORS_TABLE = "content_creators"
NEWS_TABLE = "news"
KEYWORD_TABLE = "keyword"
# Back-compat aliases for older imports; values are the new table names.
NEWS_ACCOUNTS_TABLE = NEWS_TABLE
KEYWORD_SEARCH_TABLE = KEYWORD_TABLE
PIPELINE_VERSION = "enrichment-v5.0"

# Logical column groups (same physical table; aids research vs ops clarity)
RESEARCH_COLUMNS = (
    "video_id",
    "video_url",
    "creator_username",
    "creator_display_name",
    "creator_bio",
    "creator_verified",
    "creator_followers",
    "creator_following",
    "creator_total_likes",
    "creator_video_count",
    "posted_at",
    "caption",
    "hashtags",
    "like_count",
    "comment_count",
    "share_count",
    "favorite_count",
    "video_duration_seconds",
    "voice_to_text",
    "sticker_text",
    "comments_json",
    "whisper_transcript",
    "ocr_text",
    "emoji_characters",
    "emoji_descriptions",
    "emoji_category",
)
OPERATIONAL_COLUMNS = (
    "whisper_status",
    "whisper_latency_seconds",
    "raw_ocr_text",
    "cleaned_ocr_text",
    "ocr_quality_score",
    "ocr_character_count",
    "ocr_unique_text_ratio",
    "ocr_source_count",
    "emoji_source",
    "enrichment_status",
    "enrichment_quality_score",
    "failure_reason",
    "enrichment_date",
    "pipeline_version",
)

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
        {"name": "pipeline_id", "type": "STRING"},
        {"name": "collection_source", "type": "STRING"},
    ],
    # Additive Pipeline 1 table — created only via ensure_content_creators_table()
    CONTENT_CREATORS_TABLE: [
        {"name": "video_id", "type": "STRING"},
        {"name": "video_url", "type": "STRING"},
        {"name": "creator_username", "type": "STRING"},
        {"name": "creator_display_name", "type": "STRING"},
        {"name": "creator_bio", "type": "STRING"},
        {"name": "verified_status", "type": "BOOLEAN"},
        {"name": "follower_count", "type": "INTEGER"},
        {"name": "following_count", "type": "INTEGER"},
        {"name": "total_creator_likes", "type": "INTEGER"},
        {"name": "creator_video_count", "type": "INTEGER"},
        {"name": "posted_at", "type": "STRING"},
        {"name": "caption", "type": "STRING"},
        {"name": "hashtags", "type": "STRING"},
        {"name": "likes", "type": "INTEGER"},
        {"name": "comments_count", "type": "INTEGER"},
        {"name": "shares", "type": "INTEGER"},
        {"name": "favorites", "type": "INTEGER"},
        {"name": "video_duration", "type": "FLOAT"},
        {"name": "voice_to_text", "type": "STRING"},
        {"name": "sticker_text", "type": "STRING"},
        {"name": "whisper_transcript", "type": "STRING"},
        {"name": "ocr_text", "type": "STRING"},
        {"name": "raw_ocr_text", "type": "STRING"},
        {"name": "cleaned_ocr_text", "type": "STRING"},
        {"name": "ocr_quality_score", "type": "INTEGER"},
        {"name": "emoji_characters", "type": "STRING"},
        {"name": "emoji_descriptions", "type": "STRING"},
        {"name": "emoji_category", "type": "STRING"},
        {"name": "emoji_source", "type": "STRING"},
        {"name": "emoji_count", "type": "INTEGER"},
        {"name": "view_count", "type": "INTEGER"},
        {"name": "region_code", "type": "STRING"},
        {"name": "video_mention_list", "type": "STRING"},
        {"name": "video_label", "type": "STRING"},
        {"name": "effect_ids", "type": "STRING"},
        {"name": "music_id", "type": "STRING"},
        {"name": "enrichment_status", "type": "STRING"},
        {"name": "whisper_status", "type": "STRING"},
        {"name": "ocr_status", "type": "STRING"},
        {"name": "failure_reason", "type": "STRING"},
        {"name": "pipeline_version", "type": "STRING"},
        {"name": "collection_source", "type": "STRING"},
        {"name": "collection_date", "type": "STRING"},
        {"name": "collection_window_start", "type": "STRING"},
        {"name": "collection_window_end", "type": "STRING"},
        {"name": "api_source", "type": "STRING"},
        {"name": "pipeline_id", "type": "STRING"},
        {"name": "collection_status", "type": "STRING"},
        {"name": "api_error_code", "type": "STRING"},
    ],
}

# Pipeline 2 uses the same research/enrichment columns as Pipeline 1.
BQ_SCHEMAS[NEWS_ACCOUNTS_TABLE] = [
    dict(field) for field in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]
]

# Pipeline 3 uses the same research/enrichment columns as Pipeline 1/2, plus
# matched_keywords (all keywords that matched this video_id).
BQ_SCHEMAS[KEYWORD_SEARCH_TABLE] = [
    dict(field) for field in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]
] + [{"name": "matched_keywords", "type": "STRING", "mode": "REPEATED"}]


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


def content_creators_table_id() -> str:
    return f"{gcp_project()}.{bq_dataset()}.{CONTENT_CREATORS_TABLE}"


def news_accounts_table_id() -> str:
    return f"{gcp_project()}.{bq_dataset()}.{NEWS_TABLE}"


def news_table_id() -> str:
    return news_accounts_table_id()


def keyword_search_table_id() -> str:
    return f"{gcp_project()}.{bq_dataset()}.{KEYWORD_TABLE}"


def keyword_table_id() -> str:
    return keyword_search_table_id()


def _schema_fields(table_key: str):
    from google.cloud import bigquery

    return [
        bigquery.SchemaField(
            f["name"],
            f["type"],
            mode=f.get("mode") or "NULLABLE",
        )
        for f in BQ_SCHEMAS[table_key]
    ]


def _ensure_table(client, table_key: str) -> None:
    from google.cloud import bigquery

    fields = BQ_SCHEMAS[table_key]
    table_id = f"{gcp_project()}.{bq_dataset()}.{table_key}"
    schema = _schema_fields(table_key)
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
        base_type = ddl_type.get(f["type"], f["type"])
        if (f.get("mode") or "NULLABLE") == "REPEATED":
            bq_type = f"ARRAY<{base_type}>"
        else:
            bq_type = base_type
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


def ensure_content_creators_table() -> None:
    """Create content_creators only. Does not alter tiktok_video_enriched."""
    ensure_dataset_and_tables()
    _ensure_table(_client(), CONTENT_CREATORS_TABLE)
    logger.info("BigQuery Pipeline 1 table ready: %s", content_creators_table_id())


def ensure_news_accounts_table() -> None:
    """Create news only. Does not alter v5.0 or Pipeline 1 tables."""
    ensure_dataset_and_tables()
    _ensure_table(_client(), NEWS_TABLE)
    logger.info("BigQuery Pipeline 2 table ready: %s", news_table_id())


ensure_news_table = ensure_news_accounts_table


def ensure_keyword_search_table() -> None:
    """Create keyword only. Does not alter v5.0, P1, or P2 tables."""
    ensure_dataset_and_tables()
    _ensure_table(_client(), KEYWORD_TABLE)
    logger.info("BigQuery Pipeline 3 table ready: %s", keyword_table_id())


ensure_keyword_table = ensure_keyword_search_table


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
    # Prefer actual transcript text over SQLite status flags
    transcript = (t.get("transcript") or "").strip()
    audio_available = bool(transcript)
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
    tr_error = (t.get("error") or "").strip() or _latest_worker_error(
        conn, video_id, "transcription"
    )
    # Status claimed ok but empty transcript → treat as Whisper failure
    if status == "ok" and not transcript:
        tr_error = tr_error or "empty_transcript"
    whisper_latency = _latest_worker_latency(conn, video_id, "transcription")
    vtt = (v.get("voice_to_text") or "").strip()
    sticker = (v.get("sticker_overlay_text") or "").strip()
    enrich_status, failure_reason = _enrichment_status(
        audio_available=audio_available,
        transcript_chars=len(transcript),
        ocr_frames=frames_text if has_meaningful_ocr else 0,
        transcript_error=tr_error,
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
        if not ocr_error and (bool(transcript) or bool(vtt)):
            enrich_status = "ok"
    if enrich_status == "ok" and not transcript and not vtt and not has_meaningful_ocr:
        enrich_status = "partial"
        failure_reason = failure_reason or "missing:text_layers"

    posted_at = v.get("posted_at") or (
        str(v.get("create_time") or "") if v.get("create_time") else ""
    )
    now = datetime.now(timezone.utc)
    # whisper_status must match reality: never "ok" with an empty transcript
    if transcript:
        whisper_status = "ok"
    elif status in ("error", "failed") or (status == "ok" and not transcript):
        whisper_status = "failed"
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
    extra = _sqlite_video_extra(conn, video_id)
    pipeline_id = extra.get("pipeline_id") or ""
    collection_source = extra.get("collection_source") or ""
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
                "pipeline_id": pipeline_id,
                "collection_source": collection_source,
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
            "pipeline_id": pipeline_id,
            "collection_source": collection_source,
        }
    )
    return out


def _dedupe_video_id(client, table_id: str, video_id: str) -> int:
    """Keep exactly one row per video_id (newest / highest quality wins).

    Guards against rare DELETE+INSERT races that leave duplicates.
    Returns number of duplicate rows deleted (best-effort).
    """
    from google.cloud import bigquery

    before = list(
        client.query(
            f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE video_id = @vid",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("vid", "STRING", video_id)
                ]
            ),
        ).result()
    )[0]["n"]
    if int(before or 0) <= 1:
        return 0

    # Fingerprint keep-row via ROW_NUMBER; delete the rest.
    client.query(
        f"""
        DELETE FROM `{table_id}`
        WHERE video_id = @vid
          AND TO_JSON_STRING((
            enrichment_date, enrichment_quality_score, pipeline_version,
            IFNULL(whisper_status, ''), IFNULL(ocr_text, ''), IFNULL(failure_reason, '')
          )) NOT IN (
            SELECT TO_JSON_STRING((
              enrichment_date, enrichment_quality_score, pipeline_version,
              IFNULL(whisper_status, ''), IFNULL(ocr_text, ''), IFNULL(failure_reason, '')
            ))
            FROM (
              SELECT *
              FROM `{table_id}`
              WHERE video_id = @vid
              ORDER BY enrichment_date DESC, enrichment_quality_score DESC
              LIMIT 1
            )
          )
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vid", "STRING", video_id)
            ]
        ),
    ).result()
    after = list(
        client.query(
            f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE video_id = @vid",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("vid", "STRING", video_id)
                ]
            ),
        ).result()
    )[0]["n"]
    removed = max(0, int(before or 0) - int(after or 0))
    if removed:
        logger.info("Deduped video_id=%s removed=%s remaining=%s", video_id, removed, after)
    return removed


def dedupe_all_video_ids() -> int:
    """Remove all duplicate video_id rows across the enriched table."""
    ensure_dataset_and_tables()
    from google.cloud import bigquery

    client = _client()
    table_id = enriched_table_id()
    dups = [
        r["video_id"]
        for r in client.query(
            f"""
            SELECT video_id FROM `{table_id}`
            GROUP BY video_id HAVING COUNT(*) > 1
            """
        ).result()
    ]
    removed = 0
    for vid in dups:
        removed += _dedupe_video_id(client, table_id, vid)
    return removed


def sync_video_from_sqlite(conn, video_id: str) -> Dict[str, int]:
    """Idempotent upsert: DELETE by video_id + INSERT + dedupe guard + pipeline logs."""
    ensure_dataset_and_tables()
    row = build_enriched_row(conn, video_id)
    if not row:
        logger.warning("No videos row for %s; skip BQ sync", video_id)
        return {ENRICHED_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from enrichment.validate_row import validate_enriched_row

    ok, errors = validate_enriched_row(row)
    if not ok:
        logger.error("Validation failed for %s: %s — skip BQ upload", video_id, errors)
        return {ENRICHED_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from google.cloud import bigquery

    from enrichment.store import touch_pipeline_status

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
    # Concurrent syncs can still race; enforce one row per video_id.
    _dedupe_video_id(client, table_id, video_id)

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


def _sqlite_video_extra(conn, video_id: str) -> Dict[str, Any]:
    try:
        row = conn.execute(
            """SELECT view_count, region_code, video_mention_list, video_label,
                      effect_ids, music_id, collection_source, collection_date,
                      collection_window_start, collection_window_end, pipeline_id,
                      api_source, matched_keywords
               FROM videos WHERE video_id=?""",
            (video_id,),
        ).fetchone()
    except Exception:
        row = conn.execute(
            """SELECT view_count, region_code, video_mention_list, video_label,
                      effect_ids, music_id, collection_source, collection_date,
                      collection_window_start, collection_window_end, pipeline_id,
                      api_source
               FROM videos WHERE video_id=?""",
            (video_id,),
        ).fetchone()
    return dict(row) if row else {}


def _build_handle_pipeline_row(
    conn,
    video_id: str,
    *,
    default_pipeline_id: str,
    default_api_source: str,
    default_collection_source: str,
) -> Optional[Dict[str, Any]]:
    """Map enrichment + API metadata into a handle-pipeline BigQuery schema."""
    base = build_enriched_row(conn, video_id)
    if not base:
        return None
    extra = _sqlite_video_extra(conn, video_id)
    from enrichment.emoji_extract import aggregate_emoji_fields

    em = [
        dict(r)
        for r in conn.execute(
            """SELECT emoji, emoji_name, emoji_category, count, text_source,
                      emoji_codepoint, emoji_kind
               FROM video_emojis WHERE video_id=? ORDER BY id""",
            (video_id,),
        ).fetchall()
    ]
    emoji_count = int(aggregate_emoji_fields(em).get("emoji_count") or 0)

    whisper_status = base.get("whisper_status") or "missing"
    ocr_quality = int(base.get("ocr_quality_score") or 0)
    ocr_text = (base.get("ocr_text") or "").strip()
    ocr_error = _latest_worker_error(conn, video_id, "ocr")
    if ocr_text and ocr_quality >= 25:
        ocr_status = "ok"
    elif ocr_error:
        ocr_status = "failed"
    else:
        ocr_status = "missing"

    return {
        "video_id": base["video_id"],
        "video_url": base.get("video_url") or "",
        "creator_username": base.get("creator_username") or "",
        "creator_display_name": base.get("creator_display_name") or "",
        "creator_bio": base.get("creator_bio") or "",
        "verified_status": bool(base.get("creator_verified")),
        "follower_count": base.get("creator_followers"),
        "following_count": base.get("creator_following"),
        "total_creator_likes": base.get("creator_total_likes"),
        "creator_video_count": base.get("creator_video_count"),
        "posted_at": base.get("posted_at") or "",
        "caption": base.get("caption") or "",
        "hashtags": base.get("hashtags") or "",
        "likes": base.get("like_count"),
        "comments_count": base.get("comment_count"),
        "shares": base.get("share_count"),
        "favorites": base.get("favorite_count"),
        "video_duration": base.get("video_duration_seconds"),
        "voice_to_text": base.get("voice_to_text") or "",
        "sticker_text": base.get("sticker_text") or "",
        "whisper_transcript": base.get("whisper_transcript") or "",
        "ocr_text": ocr_text,
        "raw_ocr_text": base.get("raw_ocr_text") or "",
        "cleaned_ocr_text": base.get("cleaned_ocr_text") or "",
        "ocr_quality_score": ocr_quality,
        "emoji_characters": base.get("emoji_characters") or "",
        "emoji_descriptions": base.get("emoji_descriptions") or "",
        "emoji_category": base.get("emoji_category") or "",
        "emoji_source": base.get("emoji_source") or "",
        "emoji_count": emoji_count,
        "view_count": extra.get("view_count"),
        "region_code": extra.get("region_code") or "",
        "video_mention_list": extra.get("video_mention_list") or "",
        "video_label": extra.get("video_label") or "",
        "effect_ids": extra.get("effect_ids") or "",
        "music_id": extra.get("music_id") or "",
        "enrichment_status": base.get("enrichment_status") or "",
        "whisper_status": whisper_status,
        "ocr_status": ocr_status,
        "failure_reason": base.get("failure_reason") or "",
        "pipeline_version": PIPELINE_VERSION,
        "collection_source": extra.get("collection_source") or default_collection_source,
        "collection_date": extra.get("collection_date") or "",
        "collection_window_start": extra.get("collection_window_start") or "",
        "collection_window_end": extra.get("collection_window_end") or "",
        "api_source": extra.get("api_source") or default_api_source,
        "pipeline_id": extra.get("pipeline_id") or default_pipeline_id,
        "collection_status": extra.get("collection_status") or "ok",
        "api_error_code": extra.get("api_error_code") or "",
    }


def build_content_creator_row(conn, video_id: str) -> Optional[Dict[str, Any]]:
    """Map enrichment + API metadata into the Pipeline 1 BigQuery schema."""
    return _build_handle_pipeline_row(
        conn,
        video_id,
        default_pipeline_id="content_creators",
        default_api_source="CONTENT_CREATOR_API",
        default_collection_source="content_creators",
    )


def build_news_account_row(conn, video_id: str) -> Optional[Dict[str, Any]]:
    """Map enrichment + API metadata into the Pipeline 2 BigQuery schema."""
    return _build_handle_pipeline_row(
        conn,
        video_id,
        default_pipeline_id="news",
        default_api_source="NEWS_API",
        default_collection_source="news",
    )


def build_handle_api_failure_row(
    *,
    pipeline_id: str,
    handle: str,
    collection_date: str,
    collection_window_start: str = "",
    collection_window_end: str = "",
    api_source: str = "",
    api_error_code: str = "http_error",
    failure_reason: str = "",
) -> Dict[str, Any]:
    """One P1/P2 table row for a handle whose video/query failed (no videos)."""
    from enrichment.validate_row import (
        COLLECTION_STATUS_API_FAILED,
        handle_fail_video_id,
    )

    pid = (pipeline_id or "").strip()
    name = (handle or "").strip().lstrip("@").lower()
    if pid == "news":
        default_api = "NEWS_API"
        default_source = "news"
    else:
        pid = "content_creators"
        default_api = "CONTENT_CREATOR_API"
        default_source = "content_creators"
    fields = BQ_SCHEMAS["news" if pid == "news" else CONTENT_CREATORS_TABLE]
    row: Dict[str, Any] = {}
    for f in fields:
        if f["type"] == "INTEGER":
            row[f["name"]] = 0
        elif f["type"] == "FLOAT":
            row[f["name"]] = 0.0
        elif f["type"] == "BOOLEAN":
            row[f["name"]] = False
        else:
            row[f["name"]] = ""
    row["video_id"] = handle_fail_video_id(collection_date, name)
    row["creator_username"] = name
    row["collection_date"] = collection_date
    row["collection_window_start"] = collection_window_start or ""
    row["collection_window_end"] = collection_window_end or ""
    row["collection_source"] = default_source
    row["pipeline_id"] = pid
    row["api_source"] = api_source or default_api
    row["pipeline_version"] = PIPELINE_VERSION
    row["collection_status"] = COLLECTION_STATUS_API_FAILED
    row["api_error_code"] = (api_error_code or "http_error").strip() or "http_error"
    row["failure_reason"] = (failure_reason or "").strip()[:500]
    row["enrichment_status"] = "skipped"
    return row


def upsert_handle_api_failure_row(row: Dict[str, Any]) -> int:
    """Upsert a failed-handle stub into content_creators or news."""
    if not row:
        return 0
    pid = (row.get("pipeline_id") or "").strip()
    if pid == "news":
        from enrichment.validate_row import validate_news_account_row as _validate

        ensure_news_accounts_table()
        ok, errors = _validate(row)
        table_id = news_accounts_table_id()
        loader = _load_news_account_rows
    else:
        from enrichment.validate_row import validate_pipeline_row as _validate

        ensure_content_creators_table()
        ok, errors = _validate(row)
        table_id = content_creators_table_id()
        loader = _load_content_creator_rows
    if not ok:
        logger.error("Failed-handle row invalid: %s — skip BQ upload", errors)
        return 0
    from google.cloud import bigquery

    client = _client()
    vid = row["video_id"]
    client.query(
        f"DELETE FROM `{table_id}` WHERE video_id = @vid",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("vid", "STRING", vid)]
        ),
    ).result()
    return loader([row])


def build_keyword_search_row(conn, video_id: str) -> Optional[Dict[str, Any]]:
    """Map enrichment + API metadata into the Pipeline 3 BigQuery schema."""
    from tiktok.db import parse_matched_keywords

    row = _build_handle_pipeline_row(
        conn,
        video_id,
        default_pipeline_id="keyword",
        default_api_source="KEYWORD_SEARCH_API",
        default_collection_source="keyword",
    )
    if not row:
        return None
    extra = _sqlite_video_extra(conn, video_id)
    row["matched_keywords"] = parse_matched_keywords(extra.get("matched_keywords"))
    return row


def keyword_search_schema_spec() -> Dict[str, Any]:
    """Return the Pipeline 3 schema from code. Does not create a BigQuery table."""
    fields = BQ_SCHEMAS[KEYWORD_SEARCH_TABLE]
    return {
        "table": KEYWORD_SEARCH_TABLE,
        "fields": [f["name"] for f in fields],
        "matched_keywords": next(
            (dict(f) for f in fields if f["name"] == "matched_keywords"),
            None,
        ),
        "field_count": len(fields),
    }


def _load_content_creator_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if not bigquery_configured():
        raise RuntimeError("BigQuery not configured")
    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField(f["name"], f["type"])
            for f in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]
        ],
    )
    job = client.load_table_from_json(
        rows, content_creators_table_id(), job_config=job_config
    )
    job.result()
    return len(rows)


def sync_content_creator_video(conn, video_id: str) -> Dict[str, int]:
    """Upsert one row into content_creators. Does not write tiktok_video_enriched."""
    ensure_content_creators_table()
    row = build_content_creator_row(conn, video_id)
    if not row:
        logger.warning("No videos row for %s; skip content_creators BQ sync", video_id)
        return {CONTENT_CREATORS_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from enrichment.validate_row import validate_pipeline_row

    ok, errors = validate_pipeline_row(row)
    if not ok:
        logger.error(
            "Pipeline 1 validation failed for %s: %s — skip BQ upload", video_id, errors
        )
        return {CONTENT_CREATORS_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from google.cloud import bigquery

    from enrichment.store import touch_pipeline_status

    client = _client()
    table_id = content_creators_table_id()
    client.query(
        f"DELETE FROM `{table_id}` WHERE video_id = @vid",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vid", "STRING", video_id)
            ]
        ),
    ).result()
    n = _load_content_creator_rows([row])

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

    return {CONTENT_CREATORS_TABLE: n, PIPELINE_LOGS_TABLE: n_logs}


def _load_news_account_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if not bigquery_configured():
        raise RuntimeError("BigQuery not configured")
    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField(f["name"], f["type"])
            for f in BQ_SCHEMAS[NEWS_ACCOUNTS_TABLE]
        ],
    )
    job = client.load_table_from_json(
        rows, news_accounts_table_id(), job_config=job_config
    )
    job.result()
    return len(rows)


def sync_news_account_video(conn, video_id: str) -> Dict[str, int]:
    """Upsert one row into news. Does not write tiktok_video_enriched."""
    ensure_news_accounts_table()
    row = build_news_account_row(conn, video_id)
    if not row:
        logger.warning("No videos row for %s; skip news_accounts BQ sync", video_id)
        return {NEWS_ACCOUNTS_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from enrichment.validate_row import validate_news_account_row

    ok, errors = validate_news_account_row(row)
    if not ok:
        logger.error(
            "Pipeline 2 validation failed for %s: %s — skip BQ upload", video_id, errors
        )
        return {NEWS_ACCOUNTS_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from google.cloud import bigquery

    from enrichment.store import touch_pipeline_status

    client = _client()
    table_id = news_accounts_table_id()
    client.query(
        f"DELETE FROM `{table_id}` WHERE video_id = @vid",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vid", "STRING", video_id)
            ]
        ),
    ).result()
    n = _load_news_account_rows([row])

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

    return {NEWS_ACCOUNTS_TABLE: n, PIPELINE_LOGS_TABLE: n_logs}


def _load_keyword_search_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if not bigquery_configured():
        raise RuntimeError("BigQuery not configured")
    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=_schema_fields(KEYWORD_SEARCH_TABLE),
    )
    last_err: Optional[Exception] = None
    for attempt in range(1, 6):
        try:
            job = client.load_table_from_json(
                rows, keyword_search_table_id(), job_config=job_config
            )
            job.result()
            return len(rows)
        except Exception as e:
            last_err = e
            msg = str(e)
            if "429" in msg or "rateLimitExceeded" in msg or "TooManyRequests" in type(e).__name__:
                time.sleep(wait)
                continue
            raise
    if last_err:
        raise last_err
    return 0


def _load_keyword_search_ndjson(rows: List[Dict[str, Any]]) -> int:
    """One load job from a newline-delimited JSON file (avoids per-chunk table-update quotas)."""
    if not rows:
        return 0
    if not bigquery_configured():
        raise RuntimeError("BigQuery not configured")
    import tempfile

    from google.cloud import bigquery

    client = _client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        schema=_schema_fields(KEYWORD_SEARCH_TABLE),
    )
    fd, path = tempfile.mkstemp(prefix="keyword_bq_", suffix=".ndjson")
    os.close(fd)
    try:
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        last_err: Optional[Exception] = None
        for attempt in range(1, 6):
            try:
                with open(path, "rb") as fh:
                    job = client.load_table_from_file(
                        fh, keyword_search_table_id(), job_config=job_config
                    )
                job.result()
                return len(rows)
            except Exception as e:
                last_err = e
                msg = str(e)
                if "429" in msg or "rateLimitExceeded" in msg or "TooManyRequests" in type(e).__name__:
                    wait = 45 * attempt
                    logger.warning(
                        "Keyword BQ file load rate-limited (attempt %s/5); sleep %ss",
                        attempt,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                raise
        if last_err:
            raise last_err
        return 0
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def sync_keyword_search_video(conn, video_id: str) -> Dict[str, int]:
    """Upsert one row into keyword. Does not write v5/P1/P2 tables."""
    ensure_keyword_search_table()
    row = build_keyword_search_row(conn, video_id)
    if not row:
        logger.warning("No videos row for %s; skip keyword_search BQ sync", video_id)
        return {KEYWORD_SEARCH_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from enrichment.validate_row import validate_keyword_search_row

    ok, errors = validate_keyword_search_row(row)
    if not ok:
        logger.error(
            "Pipeline 3 validation failed for %s: %s — skip BQ upload", video_id, errors
        )
        return {KEYWORD_SEARCH_TABLE: 0, PIPELINE_LOGS_TABLE: 0}

    from google.cloud import bigquery

    from enrichment.store import touch_pipeline_status

    client = _client()
    table_id = keyword_search_table_id()
    client.query(
        f"DELETE FROM `{table_id}` WHERE video_id = @vid",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vid", "STRING", video_id)
            ]
        ),
    ).result()
    n = _load_keyword_search_rows([row])

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

    return {KEYWORD_SEARCH_TABLE: n, PIPELINE_LOGS_TABLE: n_logs}


def sync_keyword_collection_date(
    conn,
    collection_date: str,
    *,
    load_chunk: int = 400,
) -> Dict[str, int]:
    """Batch-load SQLite keyword rows for one collection_date into BigQuery.

    Does not call the TikTok API. Skips video_ids already in BigQuery for that date.
    """
    from enrichment.validate_row import validate_keyword_search_row

    ensure_keyword_search_table()
    day = (collection_date or "").strip()
    if not day:
        raise ValueError("collection_date is required")

    from google.cloud import bigquery

    client = _client()
    table_id = keyword_search_table_id()
    existing = {
        str(r["video_id"])
        for r in client.query(
            f"SELECT video_id FROM `{table_id}` WHERE collection_date = @d",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("d", "STRING", day)
                ]
            ),
        ).result()
        if r["video_id"]
    }
    logger.info(
        "Keyword BQ already has %s rows for %s; those will be skipped",
        len(existing),
        day,
    )

    ids = [
        str(r[0])
        for r in conn.execute(
            """SELECT video_id FROM videos
               WHERE pipeline_id = 'keyword' AND collection_date = ?
               ORDER BY video_id""",
            (day,),
        ).fetchall()
        if r and r[0]
    ]
    built = 0
    skipped = 0
    already = 0
    valid: List[Dict[str, Any]] = []
    for i, vid in enumerate(ids, start=1):
        if vid in existing:
            already += 1
            continue
        row = build_keyword_search_row(conn, vid)
        if not row:
            skipped += 1
            continue
        ok, errors = validate_keyword_search_row(row)
        if not ok:
            skipped += 1
            if skipped <= 20:
                logger.error("Skip keyword BQ row %s: %s", vid, errors)
            continue
        valid.append(row)
        built += 1
        if i % 1000 == 0:
            logger.info(
                "Built keyword BQ rows %s/%s valid=%s skipped=%s already=%s",
                i,
                len(ids),
                built,
                skipped,
                already,
            )

    loaded = _load_keyword_search_ndjson(valid) if valid else 0

    logger.info(
        "Keyword BQ date=%s sqlite=%s already=%s valid=%s skipped=%s loaded=%s",
        day,
        len(ids),
        already,
        built,
        skipped,
        loaded,
    )
    return {
        KEYWORD_SEARCH_TABLE: loaded,
        "sqlite_ids": len(ids),
        "already": already,
        "skipped": skipped,
    }


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
