"""Convert downloaded TikTok audio to Whisper-safe formats via ffmpeg.

Always produces a temporary WAV (PCM 16-bit mono 16 kHz) for API reliability.
Original download is never retained beyond the temp directory lifetime.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


def _ffmpeg_bin(name: str = "ffmpeg") -> str:
    """Resolve ffmpeg/ffprobe, including user-local ~/bin installs."""
    import shutil

    found = shutil.which(name)
    if found:
        return found
    home = os.path.expanduser("~")
    for candidate in (
        os.path.join(home, "bin", name),
        f"/usr/local/bin/{name}",
        f"/usr/bin/{name}",
    ):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return name


@dataclass
class AudioProbe:
    path: str
    format_name: str
    codec_name: str
    duration_seconds: Optional[float]
    sample_rate: Optional[int]
    channels: Optional[int]


def probe_audio(path: str) -> AudioProbe:
    """ffprobe metadata; falls back to extension-only if ffprobe missing."""
    if not path or not os.path.isfile(path):
        return AudioProbe(path or "", "", "", None, None, None)
    try:
        out = subprocess.run(
            [
                _ffmpeg_bin("ffprobe"),
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                path,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(out.stdout or "{}")
        fmt = data.get("format") or {}
        streams = data.get("streams") or []
        audio = next((s for s in streams if s.get("codec_type") == "audio"), {}) or {}
        dur = fmt.get("duration") or audio.get("duration")
        try:
            dur_f = float(dur) if dur is not None else None
        except (TypeError, ValueError):
            dur_f = None
        sr = audio.get("sample_rate")
        ch = audio.get("channels")
        return AudioProbe(
            path=path,
            format_name=(fmt.get("format_name") or os.path.splitext(path)[1].lstrip(".") or "unknown"),
            codec_name=(audio.get("codec_name") or "unknown"),
            duration_seconds=dur_f,
            sample_rate=int(sr) if sr else None,
            channels=int(ch) if ch else None,
        )
    except Exception as e:
        logger.warning("ffprobe failed for %s: %s", path, e)
        ext = os.path.splitext(path)[1].lstrip(".") or "unknown"
        return AudioProbe(path=path, format_name=ext, codec_name="unknown", duration_seconds=None, sample_rate=None, channels=None)


def convert_for_whisper(
    src_path: str,
    *,
    prefer: str = "wav",
) -> str:
    """Convert ``src_path`` to WAV (default) or MP3 next to the source.

    Returns path to converted file. Raises RuntimeError if ffmpeg fails.
    """
    if not src_path or not os.path.isfile(src_path):
        raise RuntimeError("audio_missing")
    prefer = (prefer or "wav").lower()
    if prefer not in ("wav", "mp3"):
        prefer = "wav"
    base, _ = os.path.splitext(src_path)
    out_path = f"{base}.whisper.{prefer}"
    ff = _ffmpeg_bin("ffmpeg")
    if prefer == "wav":
        cmd = [
            ff,
            "-y",
            "-i",
            src_path,
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ac",
            "1",
            "-ar",
            "16000",
            out_path,
        ]
    else:
        cmd = [
            ff,
            "-y",
            "-i",
            src_path,
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-b:a",
            "64k",
            out_path,
        ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except FileNotFoundError as e:
        raise RuntimeError("ffmpeg_missing") from e
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode("utf-8", errors="replace")
        # Banner is long; keep the actionable tail
        tail = err[-500:] if len(err) > 500 else err
        if "does not contain any stream" in err or "Output file does not contain any stream" in err:
            raise RuntimeError("ffmpeg_no_audio_stream") from e
        raise RuntimeError(f"ffmpeg_convert_failed: {tail}") from e
    if not os.path.isfile(out_path) or os.path.getsize(out_path) < 100:
        raise RuntimeError("ffmpeg_empty_output")
    return out_path


def prepare_whisper_audio(src_path: str) -> dict:
    """Probe original → convert to WAV → probe converted.

    Returns dict with original/converted probes and ``send_path``.
    On convert failure, returns send_path=src_path with error set.
    """
    original = probe_audio(src_path)
    try:
        converted_path = convert_for_whisper(src_path, prefer="wav")
        converted = probe_audio(converted_path)
        return {
            "send_path": converted_path,
            "original": original,
            "converted": converted,
            "converted_format": "wav",
            "error": None,
        }
    except Exception as e:
        logger.warning("WAV convert failed (%s); trying MP3", e)
        try:
            converted_path = convert_for_whisper(src_path, prefer="mp3")
            converted = probe_audio(converted_path)
            return {
                "send_path": converted_path,
                "original": original,
                "converted": converted,
                "converted_format": "mp3",
                "error": None,
            }
        except Exception as e2:
            logger.error("Audio convert failed: %s / %s", e, e2)
            return {
                "send_path": src_path,
                "original": original,
                "converted": None,
                "converted_format": None,
                "error": str(e2)[:300],
            }
