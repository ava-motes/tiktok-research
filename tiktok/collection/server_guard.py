"""Refuse TikTok collection/enrichment on anything other than comm-cme-p01."""

from __future__ import annotations

import logging
import os
import socket

logger = logging.getLogger(__name__)


def require_collection_server() -> None:
    """Exit if this process is not on the Moody collection host.

    Set TIKTOK_ALLOW_LOCAL_COLLECTION=1 only for non-API unit tests.
    """
    if os.environ.get("TIKTOK_ALLOW_LOCAL_COLLECTION", "").strip() == "1":
        logger.warning("TIKTOK_ALLOW_LOCAL_COLLECTION=1 — host check bypassed")
        return
    host = socket.gethostname().lower()
    if "cme-p01" not in host:
        raise SystemExit(
            f"Refusing TikTok collection/enrichment on host {host!r}. "
            "Run on comm-cme-p01 only (Mac is for code/SSH)."
        )
