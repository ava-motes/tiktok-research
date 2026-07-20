"""Temporary media download helpers — always delete after use."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from typing import Generator, Optional

from tiktok.api.download import download_audio, download_video_file

logger = logging.getLogger(__name__)


@contextmanager
def temporary_audio(
    video_url: str,
    video_id: str,
) -> Generator[Optional[str], None, None]:
    """Download audio into a temp dir; delete the whole dir on exit."""
    tmp = tempfile.mkdtemp(prefix=f"tt_audio_{video_id}_")
    path: Optional[str] = None
    try:
        path = download_audio(video_url, video_id, tmp)
        yield path
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if path:
            logger.debug("Deleted temp audio dir for %s", video_id)


@contextmanager
def temporary_video(
    video_url: str,
    video_id: str,
) -> Generator[Optional[str], None, None]:
    """Download video into a temp dir for frame OCR; delete on exit."""
    tmp = tempfile.mkdtemp(prefix=f"tt_video_{video_id}_")
    path: Optional[str] = None
    try:
        path = download_video_file(video_url, video_id, tmp)
        yield path
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if path:
            logger.debug("Deleted temp video dir for %s", video_id)


def safe_unlink(path: Optional[str]) -> None:
    if not path:
        return
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError as e:
        logger.warning("Could not delete %s: %s", path, e)
