"""SQLite database schema, connection, and helper functions."""

import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, List

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS videos (
    video_id          TEXT PRIMARY KEY,
    username          TEXT NOT NULL,
    video_url         TEXT,
    create_time       INTEGER,
    posted_at         TEXT,
    caption           TEXT,
    hashtags          TEXT,
    like_count        INTEGER DEFAULT 0,
    share_count       INTEGER DEFAULT 0,
    comment_count     INTEGER DEFAULT 0,
    save_count        INTEGER DEFAULT 0,
    duration_seconds  INTEGER DEFAULT 0,
    voice_to_text     TEXT,
    transcript        TEXT,
    transcript_source TEXT,
    transcript_failure_reason TEXT,
    text_for_nlp      TEXT,
    news              INTEGER,
    politics          INTEGER,
    news_and_politics INTEGER,
    model_version     TEXT,
    processing_timestamp TEXT,
    inserted_at       TEXT DEFAULT (datetime('now')),
    onscreen_text     TEXT,
    onscreen_ocr_meta TEXT,
    sticker_overlay_text TEXT,
    sticker_info_list TEXT,
    browser_ocr_text  TEXT,
    visual_text_combined TEXT,
    visual_text_source_priority TEXT
);

CREATE TABLE IF NOT EXISTS users (
    username          TEXT PRIMARY KEY,
    display_name      TEXT,
    bio               TEXT,
    is_verified       INTEGER DEFAULT 0,
    follower_count    INTEGER DEFAULT 0,
    following_count   INTEGER DEFAULT 0,
    likes_count       INTEGER DEFAULT 0,
    video_count       INTEGER DEFAULT 0,
    api_failed        INTEGER DEFAULT 0,
    account_type_code INTEGER,
    account_type_label TEXT,
    model_version     TEXT,
    processing_timestamp TEXT,
    inserted_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_responses (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint          TEXT NOT NULL,
    request_params    TEXT,
    response_body     TEXT NOT NULL,
    username          TEXT,
    http_status       INTEGER,
    captured_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transcripts (
    video_id          TEXT PRIMARY KEY REFERENCES videos(video_id),
    transcript_text   TEXT,
    language          TEXT,
    transcript_source TEXT NOT NULL,
    model_name        TEXT,
    audio_path        TEXT,
    duration_seconds  REAL,
    processing_timestamp TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id        TEXT PRIMARY KEY,
    video_id          TEXT NOT NULL,
    video_url         TEXT,
    video_username    TEXT,
    commenter_handle  TEXT,
    text              TEXT,
    like_count        INTEGER DEFAULT 0,
    create_time       INTEGER,
    posted_at         TEXT,
    parent_comment_id TEXT,
    reply_count       INTEGER DEFAULT 0,
    inserted_at       TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_videos_username ON videos(username);
CREATE INDEX IF NOT EXISTS idx_videos_create_time ON videos(create_time);
CREATE INDEX IF NOT EXISTS idx_raw_responses_username ON raw_responses(username);
CREATE INDEX IF NOT EXISTS idx_comments_video_id ON comments(video_id);
CREATE INDEX IF NOT EXISTS idx_comments_video_username ON comments(video_username);
"""


def _migrate_videos_columns(conn: sqlite3.Connection) -> None:
    """Add columns for DBs created before newer video text fields existed."""
    cur = conn.execute("PRAGMA table_info(videos)")
    cols = {row[1] for row in cur.fetchall()}
    if "onscreen_text" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN onscreen_text TEXT")
    if "onscreen_ocr_meta" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN onscreen_ocr_meta TEXT")
    if "sticker_overlay_text" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN sticker_overlay_text TEXT")
    if "sticker_info_list" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN sticker_info_list TEXT")
    if "browser_ocr_text" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN browser_ocr_text TEXT")
    if "visual_text_combined" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN visual_text_combined TEXT")
    if "visual_text_source_priority" not in cols:
        conn.execute(
            "ALTER TABLE videos ADD COLUMN visual_text_source_priority TEXT"
        )
    extra = {
        "view_count": "INTEGER",
        "region_code": "TEXT",
        "video_mention_list": "TEXT",
        "video_label": "TEXT",
        "effect_ids": "TEXT",
        "music_id": "TEXT",
        "collection_source": "TEXT",
        "collection_date": "TEXT",
        "collection_window_start": "TEXT",
        "collection_window_end": "TEXT",
        "pipeline_id": "TEXT",
        "api_source": "TEXT",
    }
    for name, decl in extra.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE videos ADD COLUMN {name} {decl}")
    conn.commit()


def get_connection(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection and ensure schema exists."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    _migrate_videos_columns(conn)
    # Additive enrichment staging tables (transcripts/OCR/emojis → BigQuery)
    try:
        from tiktok.enrichment.store import ensure_enrichment_schema

        ensure_enrichment_schema(conn)
    except Exception as e:
        logger.debug("Enrichment schema skip: %s", e)
    logger.debug(f"Database ready: {db_path}")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_text_for_nlp(caption: str, transcript: str) -> str:
    """Build canonical text_for_nlp from caption and transcript."""
    parts = []
    if caption and caption.strip():
        parts.append(caption.strip())
    if transcript and transcript.strip():
        parts.append(transcript.strip())
    return "\n---\n".join(parts)


def insert_video(conn: sqlite3.Connection, video: dict):
    """Insert a video row. Duplicates are ignored, but voice_to_text and sticker
    overlay fields are updated if the API now returns them for an existing row."""
    caption = video.get("caption", "")
    voice_to_text = video.get("voice_to_text", "")
    sticker_overlay_text = video.get("sticker_overlay_text", "")
    sticker_info_list = video.get("sticker_info_list", "")
    text_for_nlp = build_text_for_nlp(caption, voice_to_text)
    transcript_source = "api" if voice_to_text else None

    conn.execute(
        """INSERT OR IGNORE INTO videos
        (video_id, username, video_url, create_time, posted_at, caption, hashtags,
         like_count, share_count, comment_count, save_count, duration_seconds,
         voice_to_text, transcript, transcript_source, text_for_nlp,
         sticker_overlay_text, sticker_info_list, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            video["video_id"],
            video["username"],
            video.get("video_url", ""),
            video.get("create_time", 0),
            video.get("posted_at", ""),
            caption,
            video.get("hashtags", ""),
            video.get("like_count", 0),
            video.get("share_count", 0),
            video.get("comment_count", 0),
            video.get("save_count", 0),
            video.get("duration_seconds", 0),
            voice_to_text,
            voice_to_text or None,  # transcript defaults to voice_to_text
            transcript_source,
            text_for_nlp,
            sticker_overlay_text or None,
            sticker_info_list or None,
            _now_iso(),
        ),
    )

    # If the API now returns voice_to_text for a video that previously had none, update it.
    if voice_to_text:
        conn.execute(
            """UPDATE videos SET voice_to_text=?, transcript=?, transcript_source=?, text_for_nlp=?
            WHERE video_id=? AND (voice_to_text IS NULL OR voice_to_text = '')""",
            (voice_to_text, voice_to_text, "api", text_for_nlp, video["video_id"]),
        )

    if sticker_overlay_text or sticker_info_list:
        conn.execute(
            """UPDATE videos SET sticker_overlay_text=?, sticker_info_list=?
            WHERE video_id=? AND (sticker_overlay_text IS NULL OR sticker_overlay_text = '')""",
            (
                sticker_overlay_text or None,
                sticker_info_list or None,
                video["video_id"],
            ),
        )


def upsert_collected_video(conn: sqlite3.Connection, video: dict) -> bool:
    """Insert or update API metadata for a collected video.

    Does not touch enrichment-only columns (onscreen_text, visual_text_*,
    classification). Returns True if the row was newly inserted.
    """
    vid = video["video_id"]
    existed = conn.execute(
        "SELECT 1 FROM videos WHERE video_id=? LIMIT 1", (vid,)
    ).fetchone()
    if not existed:
        insert_video(conn, video)
    else:
        conn.execute(
            """UPDATE videos SET
                username=?, video_url=?, create_time=?, posted_at=?, caption=?,
                hashtags=?, like_count=?, share_count=?, comment_count=?,
                save_count=?, duration_seconds=?
               WHERE video_id=?""",
            (
                video["username"],
                video.get("video_url", ""),
                video.get("create_time", 0),
                video.get("posted_at", ""),
                video.get("caption", ""),
                video.get("hashtags", ""),
                video.get("like_count", 0),
                video.get("share_count", 0),
                video.get("comment_count", 0),
                video.get("save_count", 0),
                video.get("duration_seconds", 0),
                vid,
            ),
        )
        vtt = video.get("voice_to_text") or ""
        if vtt:
            conn.execute(
                """UPDATE videos SET voice_to_text=?, transcript=?, transcript_source=?,
                       text_for_nlp=?
                   WHERE video_id=? AND (voice_to_text IS NULL OR voice_to_text = '')""",
                (
                    vtt,
                    vtt,
                    "api",
                    build_text_for_nlp(video.get("caption", ""), vtt),
                    vid,
                ),
            )
        sticker = video.get("sticker_overlay_text") or ""
        sticker_json = video.get("sticker_info_list") or ""
        if sticker or sticker_json:
            conn.execute(
                """UPDATE videos SET sticker_overlay_text=?, sticker_info_list=?
                   WHERE video_id=? AND (sticker_overlay_text IS NULL
                                         OR sticker_overlay_text = '')""",
                (sticker or None, sticker_json or None, vid),
            )

    conn.execute(
        """UPDATE videos SET
            view_count=?, region_code=?, video_mention_list=?, video_label=?,
            effect_ids=?, music_id=?, collection_source=?, collection_date=?,
            collection_window_start=?, collection_window_end=?, pipeline_id=?,
            api_source=?
           WHERE video_id=?""",
        (
            video.get("view_count"),
            video.get("region_code") or "",
            video.get("video_mention_list") or "",
            video.get("video_label") or "",
            video.get("effect_ids") or "",
            video.get("music_id") or "",
            video.get("collection_source") or "",
            video.get("collection_date") or "",
            video.get("collection_window_start") or "",
            video.get("collection_window_end") or "",
            video.get("pipeline_id") or "",
            video.get("api_source") or "",
            vid,
        ),
    )
    return existed is None


def update_video_onscreen_text(
    conn: sqlite3.Connection,
    video_id: str,
    text: str,
    meta: Optional[dict] = None,
) -> int:
    """Set ``onscreen_text`` (EasyOCR) and optional JSON meta for an existing row."""
    row = conn.execute("SELECT 1 FROM videos WHERE video_id=?", (video_id,)).fetchone()
    if not row:
        return 0
    conn.execute(
        """UPDATE videos SET onscreen_text=?, onscreen_ocr_meta=?
           WHERE video_id=?""",
        (text, json.dumps(meta) if meta is not None else None, video_id),
    )
    return 1


def update_video_browser_ocr_text(
    conn: sqlite3.Connection,
    video_id: str,
    text: str,
) -> int:
    """Set ``browser_ocr_text`` (web hydration stickersOnItem) for an existing row."""
    row = conn.execute("SELECT 1 FROM videos WHERE video_id=?", (video_id,)).fetchone()
    if not row:
        return 0
    conn.execute(
        "UPDATE videos SET browser_ocr_text=? WHERE video_id=?",
        (text, video_id),
    )
    return 1


def update_video_visual_text(
    conn: sqlite3.Connection,
    video_id: str,
    combined: str,
    source_priority: Optional[dict] = None,
) -> int:
    """Set merged ``visual_text_combined`` and provenance JSON for an existing row."""
    row = conn.execute("SELECT 1 FROM videos WHERE video_id=?", (video_id,)).fetchone()
    if not row:
        return 0
    conn.execute(
        """UPDATE videos SET visual_text_combined=?, visual_text_source_priority=?
           WHERE video_id=?""",
        (
            combined,
            json.dumps(source_priority) if source_priority is not None else None,
            video_id,
        ),
    )
    return 1


def insert_user(conn: sqlite3.Connection, user: dict):
    """Insert or replace a user row."""
    conn.execute(
        """INSERT OR REPLACE INTO users
        (username, display_name, bio, is_verified, follower_count, following_count,
         likes_count, video_count, api_failed, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user["username"],
            user.get("display_name", ""),
            user.get("bio", ""),
            int(user.get("is_verified", False)),
            user.get("follower_count", 0),
            user.get("following_count", 0),
            user.get("likes_count", 0),
            user.get("video_count", 0),
            user.get("api_failed", 0),
            _now_iso(),
        ),
    )


def insert_raw_response(conn: sqlite3.Connection, endpoint: str, username: str,
                         request_body: dict, response_body: dict, http_status: int):
    """Store a raw API response for reproducibility."""
    conn.execute(
        """INSERT INTO raw_responses (endpoint, request_params, response_body, username, http_status)
        VALUES (?, ?, ?, ?, ?)""",
        (
            endpoint,
            json.dumps(request_body, ensure_ascii=False),
            json.dumps(response_body, ensure_ascii=False),
            username,
            http_status,
        ),
    )


def insert_comment(conn: sqlite3.Connection, comment: dict):
    """Insert a comment row. Duplicates are ignored."""
    conn.execute(
        """INSERT OR IGNORE INTO comments
        (comment_id, video_id, video_url, video_username, commenter_handle, text,
         like_count, create_time, posted_at, parent_comment_id, reply_count, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            comment["comment_id"],
            comment["video_id"],
            comment.get("video_url", ""),
            comment.get("video_username", ""),
            comment.get("commenter_handle", ""),
            comment.get("text", ""),
            comment.get("like_count", 0),
            comment.get("create_time", 0),
            comment.get("posted_at", ""),
            comment.get("parent_comment_id"),
            comment.get("reply_count", 0),
            _now_iso(),
        ),
    )


def get_unclassified_videos(conn: sqlite3.Connection, usernames: Optional[List[str]] = None,
                            min_create_time: Optional[int] = None):
    """Return videos that haven't been classified yet (news_and_politics IS NULL)."""
    where = ["news IS NULL"]
    params = []

    if usernames:
        placeholders = ",".join("?" for _ in usernames)
        where.append(f"username IN ({placeholders})")
        params.extend(usernames)

    if min_create_time is not None:
        where.append("create_time >= ?")
        params.append(min_create_time)

    sql = f"SELECT * FROM videos WHERE {' AND '.join(where)}"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def update_video_classification(conn: sqlite3.Connection, video_id: str,
                                 news: int, politics: int, model_version: str):
    """Set news and politics labels for a video."""
    conn.execute(
        """UPDATE videos SET news=?, politics=?, news_and_politics=?, model_version=?, processing_timestamp=?
        WHERE video_id=?""",
        (news, politics, max(news, politics), model_version, _now_iso(), video_id),
    )


def update_user_classification(conn: sqlite3.Connection, username: str,
                                code: int, label: str, model_version: str):
    """Set account type classification for a user."""
    conn.execute(
        """UPDATE users SET account_type_code=?, account_type_label=?,
        model_version=?, processing_timestamp=? WHERE username=?""",
        (code, label, model_version, _now_iso(), username),
    )


def get_video_counts_by_user(conn: sqlite3.Connection) -> dict:
    """Aggregate total pulled and news_and_politics video counts per username."""
    rows = conn.execute(
        """SELECT username,
           COUNT(*) as videos_pulled,
           COALESCE(SUM(news_and_politics), 0) as news_and_politics_videos
        FROM videos GROUP BY username"""
    ).fetchall()
    return {r["username"]: dict(r) for r in rows}
