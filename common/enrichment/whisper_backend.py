"""Whisper transcription backends with ffmpeg pre-conversion + retry."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from enrichment.audio_convert import prepare_whisper_audio

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentTranscript:
    video_id: str
    transcript: str
    language: str
    whisper_model: str
    confidence: Optional[float] = None
    duration_seconds: Optional[float] = None
    original_audio_format: str = ""
    converted_audio_format: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)


def resolve_backend() -> str:
    """WHISPER_BACKEND=faster-whisper|openai (default: faster-whisper if installed)."""
    forced = os.environ.get("WHISPER_BACKEND", "").strip().lower()
    if forced in ("faster-whisper", "openai"):
        return forced
    try:
        import faster_whisper  # noqa: F401

        return "faster-whisper"
    except ImportError:
        return "openai"


def whisper_model_name() -> str:
    return os.environ.get("WHISPER_MODEL", "base").strip() or "base"


def portkey_provider() -> str:
    """UT Portkey Model Catalog provider slug (x-portkey-provider), if set."""
    return (
        os.environ.get("PORTKEY_PROVIDER")
        or os.environ.get("OPENAI_PORTKEY_PROVIDER")
        or ""
    ).strip()


def openai_whisper_model() -> str:
    """OpenAI/Portkey transcription model id (not faster-whisper size)."""
    explicit = (os.environ.get("OPENAI_WHISPER_MODEL") or "").strip()
    if explicit:
        return explicit
    base_url = (os.environ.get("OPENAI_BASE_URL") or "").strip().lower()
    # UT Portkey CFME workspace: provider header + plain whisper-1.
    # Catalog id @…/whisper-1 is listed but returns "invalid model ID" as model=.
    if "portkey.ai" in base_url and portkey_provider():
        return "whisper-1"
    if "portkey.ai" in base_url:
        return "@openai/whisper-1"
    return "whisper-1"


def openai_client():
    """OpenAI SDK client; optional OPENAI_BASE_URL for Portkey / UT AI Gateway."""
    from openai import OpenAI

    kwargs: Dict[str, Any] = {}
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if api_key:
        kwargs["api_key"] = api_key
    base_url = (os.environ.get("OPENAI_BASE_URL") or "").strip()
    if base_url:
        kwargs["base_url"] = base_url
    provider = portkey_provider()
    if provider and "portkey.ai" in base_url.lower():
        kwargs["default_headers"] = {"x-portkey-provider": provider}
    return OpenAI(**kwargs)


def transcribe_audio(video_id: str, audio_path: str) -> EnrichmentTranscript:
    """Convert audio to Whisper-safe format, transcribe, retry once on format errors."""
    prep = prepare_whisper_audio(audio_path)
    send_path = prep["send_path"]
    original = prep.get("original")
    converted = prep.get("converted")
    orig_fmt = ""
    if original:
        orig_fmt = f"{original.format_name}/{original.codec_name}"
    conv_fmt = prep.get("converted_format") or ""
    duration = None
    if converted and converted.duration_seconds:
        duration = converted.duration_seconds
    elif original and original.duration_seconds:
        duration = original.duration_seconds

    backend = resolve_backend()
    last_err: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            if backend == "faster-whisper":
                result = _transcribe_faster_whisper(video_id, send_path)
            else:
                result = _transcribe_openai(video_id, send_path)
            result.duration_seconds = duration or result.duration_seconds
            result.original_audio_format = orig_fmt
            result.converted_audio_format = conv_fmt or send_path.rsplit(".", 1)[-1]
            result.detail = {
                "attempt": attempt,
                "convert_error": prep.get("error"),
                "original_format": orig_fmt,
                "converted_format": result.converted_audio_format,
                "duration_seconds": result.duration_seconds,
            }
            return result
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if (
                "credit_balance" in msg
                or "insufficient_quota" in msg
                or "quota_exceeded" in msg
            ):
                logger.error(
                    "Whisper credits exhausted for %s — not retrying: %s",
                    video_id,
                    e,
                )
                break
            format_err = (
                "format_not_supported" in msg
                or "could not be decoded" in msg
                or "format is not supported" in msg
            )
            if attempt == 1 and format_err:
                # Retry with alternate container (mp3 if wav failed, else re-convert)
                logger.warning(
                    "Whisper format error for %s (attempt 1); retrying with mp3", video_id
                )
                try:
                    from enrichment.audio_convert import convert_for_whisper

                    send_path = convert_for_whisper(audio_path, prefer="mp3")
                    conv_fmt = "mp3"
                    continue
                except Exception as e2:
                    last_err = e2
                    break
            break
    raise RuntimeError(str(last_err) if last_err else "whisper_failed")


def _transcribe_faster_whisper(video_id: str, audio_path: str) -> EnrichmentTranscript:
    from faster_whisper import WhisperModel

    model_size = whisper_model_name()
    compute = os.environ.get("WHISPER_COMPUTE_TYPE", "int8").strip() or "int8"
    logger.info("faster-whisper model=%s compute=%s video_id=%s", model_size, compute, video_id)
    model = WhisperModel(model_size, device="cpu", compute_type=compute)
    segments, info = model.transcribe(audio_path, beam_size=1, vad_filter=True)
    texts = []
    probs = []
    for seg in segments:
        texts.append(seg.text.strip())
        if seg.avg_logprob is not None:
            probs.append(max(0.0, min(1.0, float(seg.avg_logprob) + 1.0)))
    text = " ".join(t for t in texts if t).strip()
    conf = sum(probs) / len(probs) if probs else None
    dur = getattr(info, "duration", None)
    try:
        dur_f = float(dur) if dur is not None else None
    except (TypeError, ValueError):
        dur_f = None
    return EnrichmentTranscript(
        video_id=video_id,
        transcript=text,
        language=getattr(info, "language", "") or "",
        whisper_model=f"faster-whisper-{model_size}",
        confidence=conf,
        duration_seconds=dur_f,
    )


def _transcribe_openai(video_id: str, audio_path: str) -> EnrichmentTranscript:
    from enrichment.retry import with_retries

    model = openai_whisper_model()
    max_bytes = 25 * 1024 * 1024

    def _once() -> EnrichmentTranscript:
        size = os.path.getsize(audio_path) if os.path.isfile(audio_path) else 0
        if size > max_bytes:
            raise RuntimeError("too_large")
        client = openai_client()
        with open(audio_path, "rb") as handle:
            response = client.audio.transcriptions.create(model=model, file=handle)
        text = (getattr(response, "text", None) or "").strip()
        return EnrichmentTranscript(
            video_id=video_id,
            transcript=text,
            language=getattr(response, "language", None) or "",
            whisper_model=f"openai-{model}",
            confidence=None,
            duration_seconds=None,
        )

    return with_retries(_once, attempts=3, label=f"whisper:{video_id}")
