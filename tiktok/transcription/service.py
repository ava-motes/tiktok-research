"""Transcription service using faster-whisper (local) or OpenAI Whisper API."""

import os
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

OPENAI_WHISPER_MAX_BYTES = 25 * 1024 * 1024  # 25 MB API limit


@dataclass
class TranscriptResult:
    video_id: str
    text: str
    language: str
    source: str                  # 'api' or 'asr'
    model_name: Optional[str]    # e.g. 'faster-whisper-base'
    audio_path: Optional[str]    # e.g. 'audio/7321456789.mp3'
    duration_seconds: Optional[float]


class TranscriptionService:
    """Transcribes TikTok videos using faster-whisper (local ASR).

    Usage:
        svc = TranscriptionService(model_size="base", audio_dir="audio")
        result = svc.transcribe(video_id="7321456789", audio_path="audio/7321456789.mp3")

    For videos that already have TikTok's voice_to_text field,
    use TranscriptionService.from_api_transcript() instead.
    """

    def __init__(self, model_size: str = "base", audio_dir: str = "audio",
                 compute_type: str = "int8"):
        self.model_size = model_size
        self.audio_dir = audio_dir
        self.compute_type = compute_type
        self._model = None

    def _load_model(self):
        if self._model is None:
            from faster_whisper import WhisperModel
            logger.info(f"Loading faster-whisper model: {self.model_size} ({self.compute_type})")
            self._model = WhisperModel(self.model_size, compute_type=self.compute_type)
        return self._model

    def transcribe(self, video_id: str, audio_path: str) -> Optional[TranscriptResult]:
        """Transcribe a single audio file."""
        if not os.path.exists(audio_path):
            logger.warning(f"Audio file not found: {audio_path}")
            return None

        try:
            model = self._load_model()
            segments, info = model.transcribe(audio_path)
            text = " ".join(seg.text.strip() for seg in segments)

            return TranscriptResult(
                video_id=video_id,
                text=text,
                language=info.language,
                source="asr",
                model_name=f"faster-whisper-{self.model_size}",
                audio_path=audio_path,
                duration_seconds=info.duration,
            )
        except Exception as e:
            logger.error(f"Transcription failed for {video_id}: {e}")
            return None

    @staticmethod
    def from_api_transcript(video_id: str, voice_to_text: str) -> TranscriptResult:
        """Wrap TikTok's built-in voice_to_text as a TranscriptResult."""
        return TranscriptResult(
            video_id=video_id,
            text=voice_to_text,
            language="",
            source="api",
            model_name=None,
            audio_path=None,
            duration_seconds=None,
        )


class WhisperAPITranscriptionService:
    """Transcribes audio files using the OpenAI Whisper API.

    Requires OPENAI_API_KEY in the environment. Files over 25 MB are skipped.

    Usage:
        svc = WhisperAPITranscriptionService()
        result = svc.transcribe(video_id="7321456789", audio_path="audio/7321456789.mp3")
    """

    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is None:
            import openai
            self._client = openai.OpenAI()
        return self._client

    def transcribe(self, video_id: str, audio_path: str,
                   error_info: dict = None) -> Optional[TranscriptResult]:
        """Transcribe a single audio file via the OpenAI Whisper API.

        If error_info dict is provided, populates error_info['reason'] with a
        categorized failure code on failure:
            'ffmpeg_missing'       — ffmpeg not installed (needed for large files)
            'too_large'            — file too large even after compression
            'format_not_supported' — Whisper cannot decode the audio format
            'transcription_failed' — other Whisper API error
        """
        if not os.path.exists(audio_path):
            logger.warning(f"Audio file not found: {audio_path}")
            if error_info is not None:
                error_info["reason"] = "transcription_failed"
            return None

        file_size = os.path.getsize(audio_path)
        compressed_path = None

        if file_size > OPENAI_WHISPER_MAX_BYTES:
            compressed_path = audio_path + ".compressed.mp3"
            logger.info(f"Compressing {video_id} ({file_size / 1024 / 1024:.1f} MB) to 32kbps mono")
            try:
                import subprocess
                import shutil
                ffmpeg_bin = shutil.which("ffmpeg")
                if not ffmpeg_bin:
                    try:
                        import imageio_ffmpeg
                        ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
                    except ImportError:
                        pass
                if not ffmpeg_bin:
                    raise RuntimeError("ffmpeg not found")
                subprocess.run(
                    [ffmpeg_bin, "-y", "-i", audio_path, "-ac", "1", "-b:a", "32k", compressed_path],
                    check=True, capture_output=True,
                )
            except Exception as e:
                logger.error(f"Compression failed for {video_id}: {e}")
                if error_info is not None:
                    msg = str(e)
                    error_info["reason"] = "ffmpeg_missing" if "ffmpeg" in msg.lower() else "transcription_failed"
                return None

            new_size = os.path.getsize(compressed_path)
            if new_size > OPENAI_WHISPER_MAX_BYTES:
                logger.warning(f"Skipping {video_id}: still too large after compression ({new_size / 1024 / 1024:.1f} MB)")
                os.remove(compressed_path)
                if error_info is not None:
                    error_info["reason"] = "too_large"
                return None

            send_path = compressed_path
        else:
            send_path = audio_path

        try:
            client = self._get_client()
            with open(send_path, "rb") as f:
                response = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                )
            return TranscriptResult(
                video_id=video_id,
                text=response.text,
                language="",
                source="asr",
                model_name="openai-whisper-1",
                audio_path=audio_path,
                duration_seconds=None,
            )
        except Exception as e:
            logger.error(f"OpenAI transcription failed for {video_id}: {e}")
            if error_info is not None:
                msg = str(e)
                if "could not be decoded" in msg or "format is not supported" in msg:
                    error_info["reason"] = "format_not_supported"
                else:
                    error_info["reason"] = "transcription_failed"
            return None
        finally:
            if compressed_path and os.path.exists(compressed_path):
                os.remove(compressed_path)
