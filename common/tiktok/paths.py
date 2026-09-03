"""Repository root helpers. Prefer ``common.bootstrap`` from entry scripts."""

from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here.parent, *here.parents]:
        if (p / "common" / "tiktok").is_dir() and (p / "p1_content_creators").is_dir():
            return p
    raise RuntimeError("tiktok_research repo root not found")
