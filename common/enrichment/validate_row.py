"""Pre-BigQuery validation for final enriched-row schema."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

REQUIRED_FIELDS = ("video_id", "creator_username")

HANDLE_FAIL_VIDEO_PREFIX = "handle_fail:"
COLLECTION_STATUS_OK = "ok"
COLLECTION_STATUS_API_FAILED = "api_failed"


def handle_fail_video_id(collection_date: str, handle: str) -> str:
    """Stable identity for a handle that produced no videos on this research date."""
    day = (collection_date or "").strip()
    name = (handle or "").strip().lstrip("@").lower()
    return f"{HANDLE_FAIL_VIDEO_PREFIX}{day}:{name}"


def is_handle_fail_video_id(video_id: Any) -> bool:
    return str(video_id or "").startswith(HANDLE_FAIL_VIDEO_PREFIX)


def validate_enriched_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    if not row:
        return False, ["empty_row"]
    for field in REQUIRED_FIELDS:
        val = row.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            errors.append(f"missing_{field}")
    vid = str(row.get("video_id") or "")
    status = str(row.get("collection_status") or "").strip()
    if status == COLLECTION_STATUS_API_FAILED or is_handle_fail_video_id(vid):
        if not is_handle_fail_video_id(vid):
            errors.append("handle_fail_video_id_invalid")
        return (len(errors) == 0), errors
    if vid and not vid.isdigit():
        errors.append("video_id_not_numeric")
    return (len(errors) == 0), errors


def validate_pipeline_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate a Pipeline 1 (content_creators) BigQuery row before upsert."""
    ok, errors = validate_enriched_row(row)
    source = (row.get("collection_source") or "").strip()
    if source and source != "content_creators":
        errors.append("unexpected_collection_source")
    pid = (row.get("pipeline_id") or "").strip()
    if pid and pid != "content_creators":
        errors.append("unexpected_pipeline_id")
    return (len(errors) == 0), errors


def validate_news_account_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate a Pipeline 2 (news) BigQuery row before upsert."""
    ok, errors = validate_enriched_row(row)
    source = (row.get("collection_source") or "").strip()
    if source and source != "news":
        errors.append("unexpected_collection_source")
    pid = (row.get("pipeline_id") or "").strip()
    if pid and pid != "news":
        errors.append("unexpected_pipeline_id")
    return (len(errors) == 0), errors


validate_news_row = validate_news_account_row


def validate_keyword_search_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate a Pipeline 3 (keyword) BigQuery row before upsert."""
    ok, errors = validate_enriched_row(row)
    source = (row.get("collection_source") or "").strip()
    if source and source != "keyword":
        errors.append("unexpected_collection_source")
    pid = (row.get("pipeline_id") or "").strip()
    if pid and pid != "keyword":
        errors.append("unexpected_pipeline_id")
    mk = row.get("matched_keywords")
    if not isinstance(mk, list) or not [str(x).strip() for x in mk if str(x).strip()]:
        errors.append("missing_matched_keywords")
    return (len(errors) == 0), errors
