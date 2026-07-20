"""Download audio from TikTok videos using yt-dlp."""

import glob
import logging
import os
import shutil
from typing import Optional

logger = logging.getLogger(__name__)


def _ffmpeg_dir() -> Optional[str]:
    """Directory containing ffmpeg/ffprobe (supports ~/bin user installs)."""
    home_bin = os.path.join(os.path.expanduser("~"), "bin")
    for candidate in (
        shutil.which("ffmpeg"),
        os.path.join(home_bin, "ffmpeg"),
        "/usr/local/bin/ffmpeg",
        "/usr/bin/ffmpeg",
    ):
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            d = os.path.dirname(os.path.abspath(candidate))
            path = os.environ.get("PATH", "")
            if d and d not in path.split(os.pathsep):
                os.environ["PATH"] = d + os.pathsep + path
            return d
    return None


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

    Downloads the best available native audio stream (no yt-dlp extract).
    Whisper conversion to WAV/MP3 happens later in ``audio_convert``.

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

    # Prefer muxed formats that actually include an audio track.
    # TikTok HEVC "best" sometimes reports aac but ships video-only — avoid that.
    format_candidates = [
        # Prefer H.264 muxes — some TikTok HEVC "best" files claim AAC but ship video-only.
        "best[acodec!=none][vcodec=h264]/best[acodec!=none][vcodec^=avc1]/best[acodec!=none][vcodec!=hevc][vcodec!=h265]/bestaudio/best",
        "best[acodec!=none][vcodec=h264]/bestaudio/best",
    ]
    ffdir = _ffmpeg_dir()
    last_err: Optional[Exception] = None

    for fmt in format_candidates:
        ydl_opts = {
            "format": fmt,
            "outtmpl": os.path.join(audio_dir, f"{video_id}.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
        }
        if ffdir:
            ydl_opts["ffmpeg_location"] = ffdir
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            result = _find_existing(audio_dir, video_id)
            if result and _file_has_audio(result):
                return result
            if result:
                # Video-only mux — delete and try next format selector
                logger.warning(
                    "Downloaded media for %s has no audio stream; retrying format",
                    video_id,
                )
                try:
                    os.remove(result)
                except OSError:
                    pass
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        msg = str(last_err)
        if error_info is not None:
            if "not comfortable for some audiences" in msg or "Log in for access" in msg:
                error_info["reason"] = "content_warning"
            elif "No video formats found" in msg:
                error_info["reason"] = "no_formats"
            elif "Failed to resolve" in msg or "getaddrinfo" in msg:
                error_info["reason"] = "dns_error"
            else:
                error_info["reason"] = "download_failed"
        logger.warning(f"Download failed for {video_id}: {last_err}")
    else:
        if error_info is not None:
            error_info["reason"] = "no_audio_stream"
        logger.warning("No audio stream available for %s", video_id)
    return None


def _file_has_audio(path: str) -> bool:
    """Return True if ffprobe reports at least one audio stream."""
    if not path or not os.path.isfile(path):
        return False
    ffprobe = os.path.join(_ffmpeg_dir() or "", "ffprobe") if _ffmpeg_dir() else "ffprobe"
    if not os.path.isfile(ffprobe):
        ffprobe = shutil.which("ffprobe") or "ffprobe"
    try:
        import subprocess

        out = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-select_streams",
                "a",
                "-show_entries",
                "stream=codec_type",
                "-of",
                "csv=p=0",
                path,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        return "audio" in (out.stdout or "").lower()
    except Exception:
        # If we cannot probe, allow the file through (Whisper path may still work)
        return True


def download_video_file(video_url: str, video_id: str, out_dir: str) -> Optional[str]:
    """Download best video+audio (mp4 preferred) for OCR / frame extraction.

    Returns path to the video file, or None on failure. Skips if a video file
    already exists for ``video_id`` (mp4/mkv/webm).
    """
    os.makedirs(out_dir, exist_ok=True)

    for ext in (".mp4", ".mkv", ".webm", ".mov"):
        p = os.path.join(out_dir, f"{video_id}{ext}")
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            logger.debug("Using existing video file: %s", p)
            return p

    try:
        import yt_dlp
    except ImportError:
        logger.error("yt-dlp not installed")
        return None

    outtmpl = os.path.join(out_dir, f"{video_id}.%(ext)s")
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
    }
    ffdir = _ffmpeg_dir()
    if ffdir:
        ydl_opts["ffmpeg_location"] = ffdir

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
    except Exception as e:
        logger.warning("Video download failed for %s: %s", video_id, e)
        return None

    for ext in (".mp4", ".mkv", ".webm", ".mov"):
        p = os.path.join(out_dir, f"{video_id}{ext}")
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            return p
    return None


def extract_video_metadata(url: str) -> Optional[dict]:
    """Return yt-dlp info dict (id, title, description, uploader, ...) without downloading."""
    try:
        import yt_dlp
    except ImportError:
        return None
    ydl_opts = {"quiet": True, "no_warnings": True, "noplaylist": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception as e:
        logger.warning("extract_info failed for %s: %s", url, e)
        return None
