"""SQLite staging tables for enrichment → BigQuery sync.

Additive schema only — never alters Research API collection tables' meaning.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

ENRICHMENT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS video_transcripts (
    video_id              TEXT PRIMARY KEY,
    transcript            TEXT,
    language              TEXT,
    whisper_model         TEXT,
    confidence            REAL,
    processing_timestamp  TEXT,
    status                TEXT,
    error                 TEXT
);

CREATE TABLE IF NOT EXISTS video_ocr (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id              TEXT NOT NULL,
    frame_timestamp       REAL,
    frame_number          INTEGER,
    ocr_text              TEXT,
    confidence            REAL,
    source                TEXT DEFAULT 'google_vision',
    processing_timestamp  TEXT
);

CREATE INDEX IF NOT EXISTS idx_video_ocr_video_id ON video_ocr(video_id);

CREATE TABLE IF NOT EXISTS video_emojis (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id              TEXT NOT NULL,
    text_source           TEXT,
    emoji                 TEXT NOT NULL,
    emoji_name            TEXT,
    emoji_category        TEXT,
    count                 INTEGER DEFAULT 1,
    processing_timestamp  TEXT
);

CREATE INDEX IF NOT EXISTS idx_video_emojis_video_id ON video_emojis(video_id);

CREATE TABLE IF NOT EXISTS enrichment_log (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    worker                TEXT NOT NULL,
    video_id              TEXT NOT NULL,
    ok                    INTEGER NOT NULL,
    started_at            TEXT,
    ended_at              TEXT,
    elapsed_seconds       REAL,
    error                 TEXT,
    detail_json           TEXT
);

CREATE TABLE IF NOT EXISTS enrichment_pipeline_status (
    video_id                 TEXT PRIMARY KEY,
    collection_started       TEXT,
    collection_completed     TEXT,
    ocr_started              TEXT,
    ocr_completed            TEXT,
    transcription_started    TEXT,
    transcription_completed  TEXT,
    emoji_started            TEXT,
    emoji_completed          TEXT,
    bq_uploaded              TEXT,
    updated_at               TEXT
);

CREATE TABLE IF NOT EXISTS video_ocr_stats (
    video_id                    TEXT PRIMARY KEY,
    number_of_frames_processed  INTEGER,
    frames_with_text            INTEGER,
    average_text_per_frame      REAL,
    ocr_confidence_avg          REAL,
    ocr_language                TEXT,
    processing_timestamp        TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def ensure_enrichment_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(ENRICHMENT_SCHEMA_SQL)
    # Additive OCR columns for post-processed / structured text
    _ensure_column(conn, "video_ocr", "ocr_text_raw", "TEXT")
    _ensure_column(conn, "video_ocr", "ocr_text_clean", "TEXT")
    _ensure_column(conn, "video_ocr", "frame_pct", "REAL")
    _ensure_column(conn, "video_ocr", "source_type", "TEXT")
    # Whisper audio metadata
    _ensure_column(conn, "video_transcripts", "audio_duration_seconds", "REAL")
    _ensure_column(conn, "video_transcripts", "original_audio_format", "TEXT")
    _ensure_column(conn, "video_transcripts", "converted_audio_format", "TEXT")
    # Emoji taxonomy
    _ensure_column(conn, "video_emojis", "emoji_codepoint", "TEXT")
    _ensure_column(conn, "video_emojis", "emoji_kind", "TEXT")
    conn.commit()


def upsert_transcript(
    conn: sqlite3.Connection,
    *,
    video_id: str,
    transcript: str,
    language: str = "",
    whisper_model: str = "",
    confidence: Optional[float] = None,
    status: str = "ok",
    error: Optional[str] = None,
    audio_duration_seconds: Optional[float] = None,
    original_audio_format: str = "",
    converted_audio_format: str = "",
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO video_transcripts
        (video_id, transcript, language, whisper_model, confidence,
         processing_timestamp, status, error,
         audio_duration_seconds, original_audio_format, converted_audio_format)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            video_id,
            transcript,
            language,
            whisper_model,
            confidence,
            _now(),
            status,
            error,
            audio_duration_seconds,
            original_audio_format or "",
            converted_audio_format or "",
        ),
    )


def upsert_ocr_stats(
    conn: sqlite3.Connection,
    video_id: str,
    *,
    number_of_frames_processed: int = 0,
    frames_with_text: int = 0,
    average_text_per_frame: Optional[float] = None,
    ocr_confidence_avg: Optional[float] = None,
    ocr_language: str = "",
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO video_ocr_stats
        (video_id, number_of_frames_processed, frames_with_text,
         average_text_per_frame, ocr_confidence_avg, ocr_language, processing_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            video_id,
            int(number_of_frames_processed or 0),
            int(frames_with_text or 0),
            average_text_per_frame,
            ocr_confidence_avg,
            ocr_language or "",
            _now(),
        ),
    )


def replace_ocr_rows(conn: sqlite3.Connection, video_id: str, rows: List[Dict[str, Any]]) -> int:
    conn.execute("DELETE FROM video_ocr WHERE video_id=?", (video_id,))
    ts = _now()
    for r in rows:
        raw = r.get("ocr_text_raw") or r.get("ocr_text") or ""
        clean = r.get("ocr_text_clean") or r.get("ocr_text") or raw
        conn.execute(
            """INSERT INTO video_ocr
            (video_id, frame_timestamp, frame_number, ocr_text, confidence, source,
             processing_timestamp, ocr_text_raw, ocr_text_clean, frame_pct, source_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id,
                r.get("frame_timestamp"),
                r.get("frame_number"),
                clean,  # working text = cleaned
                r.get("confidence"),
                r.get("source") or "google_vision",
                ts,
                raw,
                clean,
                r.get("frame_pct"),
                r.get("source_type") or "",
            ),
        )
    return len(rows)


def replace_emoji_rows(conn: sqlite3.Connection, video_id: str, rows: List[Dict[str, Any]]) -> int:
    conn.execute("DELETE FROM video_emojis WHERE video_id=?", (video_id,))
    ts = _now()
    for r in rows:
        conn.execute(
            """INSERT INTO video_emojis
            (video_id, text_source, emoji, emoji_name, emoji_category, count,
             processing_timestamp, emoji_codepoint, emoji_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id,
                r.get("text_source") or "",
                r.get("emoji") or "",
                r.get("emoji_name") or "",
                r.get("emoji_category") or "",
                int(r.get("count") or 1),
                ts,
                r.get("emoji_codepoint") or "",
                r.get("emoji_kind") or "",
            ),
        )
    return len(rows)


def touch_pipeline_status(conn: sqlite3.Connection, video_id: str, **fields: Optional[str]) -> None:
    """Upsert enrichment_pipeline_status timestamps (ISO strings)."""
    if not video_id:
        return
    now = _now()
    conn.execute(
        """INSERT INTO enrichment_pipeline_status (video_id, updated_at)
           VALUES (?, ?)
           ON CONFLICT(video_id) DO UPDATE SET updated_at=excluded.updated_at""",
        (video_id, now),
    )
    allowed = {
        "collection_started",
        "collection_completed",
        "ocr_started",
        "ocr_completed",
        "transcription_started",
        "transcription_completed",
        "emoji_started",
        "emoji_completed",
        "bq_uploaded",
    }
    for key, val in fields.items():
        if key in allowed and val is not None:
            conn.execute(
                f"UPDATE enrichment_pipeline_status SET {key}=?, updated_at=? WHERE video_id=?",
                (val, now, video_id),
            )


def insert_enrichment_log(conn: sqlite3.Connection, result: Dict[str, Any]) -> None:
    conn.execute(
        """INSERT INTO enrichment_log
        (worker, video_id, ok, started_at, ended_at, elapsed_seconds, error, detail_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            result.get("worker"),
            result.get("video_id"),
            1 if result.get("ok") else 0,
            result.get("started_at"),
            result.get("ended_at"),
            result.get("elapsed_seconds"),
            result.get("error"),
            json.dumps(result.get("detail") or {}, ensure_ascii=False),
        ),
    )


def fetch_videos_for_enrichment(
    conn: sqlite3.Connection,
    *,
    handles: Optional[List[str]] = None,
    video_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
    need_transcript: bool = False,
    need_ocr: bool = False,
    need_emoji: bool = False,
) -> List[Dict[str, Any]]:
    """Select candidate videos from the existing videos table."""
    conditions: List[str] = []
    params: List[Any] = []

    if video_ids:
        ph = ",".join("?" for _ in video_ids)
        conditions.append(f"v.video_id IN ({ph})")
        params.extend(video_ids)
    if handles:
        ph = ",".join("?" for _ in handles)
        conditions.append(f"v.username IN ({ph})")
        params.extend(handles)

    if need_transcript:
        conditions.append(
            """v.video_id NOT IN (
                 SELECT video_id FROM video_transcripts WHERE status='ok' AND transcript IS NOT NULL
                   AND length(transcript) > 0
               )"""
        )
    if need_ocr:
        conditions.append(
            "v.video_id NOT IN (SELECT DISTINCT video_id FROM video_ocr)"
        )
    if need_emoji:
        conditions.append(
            "v.video_id NOT IN (SELECT DISTINCT video_id FROM video_emojis)"
        )

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT v.video_id, v.username, v.video_url, v.caption, v.hashtags,
               v.voice_to_text, v.transcript, v.sticker_overlay_text,
               v.browser_ocr_text, v.onscreen_text, v.visual_text_combined,
               v.duration_seconds
        FROM videos v
        {where}
        ORDER BY v.create_time DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    return [dict(r) for r in conn.execute(sql, params).fetchall()]
