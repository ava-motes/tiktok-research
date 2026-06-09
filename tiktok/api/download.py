"""Download audio from TikTok videos using yt-dlp."""

import os
import glob
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _find_existing(audio_dir: str, video_id: str) -> Optional[str]:
    """Check if an audio file already exists for this video (any extension)."""
    matches = glob.glob(os.path.join(audio_dir, f"{video_id}.*"))
    for m in matches:
        if m.endswith((".mp3", ".m4a", ".mp4", ".webm", ".opus", ".ogg", ".wav")):
            return m
    return None


def download_audio(video_url: str, video_id: str, audio_dir: str,
                   error_info: dict = None) -> Optional[str]:
    """Download audio from a TikTok video URL.

    Downloads the best available audio stream. If ffmpeg is available,
    converts to MP3; otherwise keeps the native format (usually m4a).

    Returns the output file path on success, None on failure.
    Skips download if a file already exists.

    If error_info dict is provided, populates error_info['reason'] with a
    categorized failure code on failure:
        'content_warning'  — TikTok requires login to view this content
        'no_formats'       — No downloadable video formats found
        'dns_error'        — DNS resolution failure (transient)
        'download_failed'  — Other download error
    """
    os.makedirs(audio_dir, exist_ok=True)

    existing = _find_existing(audio_dir, video_id)
    if existing:
        return existing

    try:
        import yt_dlp
    except ImportError:
        logger.error("yt-dlp not installed. Install with: pip install yt-dlp")
        return None

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(audio_dir, f"{video_id}.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "5",
        }],
    }

    # yt-dlp postprocessing requires both ffmpeg and ffprobe on PATH.
    # imageio-ffmpeg only bundles ffmpeg, not ffprobe, so skip postprocessing
    # unless a full system ffmpeg installation is present.
    import shutil
    if not shutil.which("ffmpeg"):
        ydl_opts.pop("postprocessors")

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        result = _find_existing(audio_dir, video_id)
        if result:
            return result

        logger.warning(f"yt-dlp completed but no audio file found for {video_id}")
        return None

    except Exception as e:
        msg = str(e)
        if error_info is not None:
            if "not comfortable for some audiences" in msg or "Log in for access" in msg:
                error_info["reason"] = "content_warning"
            elif "No video formats found" in msg:
                error_info["reason"] = "no_formats"
            elif "Failed to resolve" in msg or "getaddrinfo" in msg:
                error_info["reason"] = "dns_error"
            else:
                error_info["reason"] = "download_failed"
        logger.warning(f"Download failed for {video_id}: {e}")
        return None
