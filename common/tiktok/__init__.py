"""Shared TikTok research package (auth, db, collection, Box).

API client: ``common/api`` (import as ``api`` or ``tiktok.api``).
Enrichment: ``common/enrichment`` (import as ``enrichment`` or ``tiktok.enrichment``).
"""

from __future__ import annotations

import sys
from pathlib import Path

_common = Path(__file__).resolve().parent.parent
if str(_common) not in sys.path:
    sys.path.insert(0, str(_common))

import api as _api_pkg  # noqa: E402
import enrichment as _enrichment_pkg  # noqa: E402

sys.modules[__name__ + ".api"] = _api_pkg
sys.modules[__name__ + ".enrichment"] = _enrichment_pkg
