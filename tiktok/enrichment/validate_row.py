"""Pre-BigQuery validation for final enriched-row schema."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

REQUIRED_FIELDS = ("video_id", "creator_username")


def validate_enriched_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    if not row:
        return False, ["empty_row"]
    for field in REQUIRED_FIELDS:
        val = row.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            errors.append(f"missing_{field}")
    vid = str(row.get("video_id") or "")
    if vid and not vid.isdigit():
        errors.append("video_id_not_numeric")
    return (len(errors) == 0), errors
