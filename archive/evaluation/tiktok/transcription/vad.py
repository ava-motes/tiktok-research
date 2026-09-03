"""Voice Activity Detection using webrtcvad.

Detects what fraction of an audio file contains speech.
Used to skip transcription for videos with little/no speech (music, silence, etc).
"""

import logging
import struct
import wave
import tempfile
import os

logger = logging.getLogger(__name__)

SAMPLE_RATE = 16000   # webrtcvad requires 8000, 16000, or 32000 Hz
FRAME_MS = 30         # webrtcvad supports 10, 20, or 30ms frames
FRAME_BYTES = int(SAMPLE_RATE * FRAME_MS / 1000) * 2  # 16-bit mono = 2 bytes/sample


def _get_ffmpeg() -> str:
    """Return path to ffmpeg binary, preferring imageio_ffmpeg bundled binary."""
    import shutil
    system_ff = shutil.which("ffmpeg")
    if system_ff:
        return system_ff
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        raise RuntimeError("ffmpeg not found. Install imageio-ffmpeg: pip install imageio-ffmpeg")


def _convert_to_pcm(audio_path: str) -> str:
    """Convert audio to 16kHz mono WAV using ffmpeg. Returns path to temp WAV file."""
    import subprocess
    ffmpeg = _get_ffmpeg()
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    subprocess.run(
        [ffmpeg, "-y", "-i", audio_path,
         "-ar", str(SAMPLE_RATE), "-ac", "1", "-sample_fmt", "s16",
         tmp.name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )
    return tmp.name


def speech_ratio(audio_path: str, aggressiveness: int = 2) -> float:
    """Return the fraction of 30ms frames classified as speech (0.0–1.0).

    aggressiveness: 0 (least) to 3 (most aggressive filtering of non-speech).
    """
    import webrtcvad
    vad = webrtcvad.Vad(aggressiveness)

    wav_path = None
    try:
        wav_path = _convert_to_pcm(audio_path)
        with wave.open(wav_path, "rb") as wf:
            raw = wf.readframes(wf.getnframes())

        frames = [
            raw[i:i + FRAME_BYTES]
            for i in range(0, len(raw) - FRAME_BYTES + 1, FRAME_BYTES)
        ]
        if not frames:
            return 0.0

        speech_frames = sum(
            1 for f in frames if len(f) == FRAME_BYTES and vad.is_speech(f, SAMPLE_RATE)
        )
        return speech_frames / len(frames)

    except Exception as e:
        logger.warning(f"VAD failed for {audio_path}: {e}")
        return 1.0  # assume speech on error so we don't skip valid videos
    finally:
        if wav_path and os.path.exists(wav_path):
            os.remove(wav_path)


def has_enough_speech(audio_path: str, min_ratio: float = 0.10, aggressiveness: int = 2) -> bool:
    """Return True if the audio has at least min_ratio speech content."""
    ratio = speech_ratio(audio_path, aggressiveness)
    logger.debug(f"VAD speech ratio: {ratio:.2%} for {os.path.basename(audio_path)}")
    return ratio >= min_ratio
