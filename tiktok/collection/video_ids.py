"""Helpers for scoping enrichment workers to collected video_ids."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import List, Optional


def add_video_id_args(parser: ArgumentParser) -> None:
    """Additive CLI: keep --video-id, add optional --video-ids-file."""
    parser.add_argument("--video-id", default=None, help="Single video id")
    parser.add_argument(
        "--video-ids-file",
        default=None,
        help="Text file with one video_id per line (Pipeline 1 daily runs)",
    )


def resolve_video_ids(args: Namespace) -> Optional[List[str]]:
    ids: List[str] = []
    single = getattr(args, "video_id", None)
    if single:
        ids.append(str(single).strip())
    path = getattr(args, "video_ids_file", None)
    if path:
        text = Path(path).read_text(encoding="utf-8")
        for line in text.splitlines():
            vid = line.strip()
            if vid and not vid.startswith("#"):
                ids.append(vid)
    out = [v for v in ids if v]
    return out or None
