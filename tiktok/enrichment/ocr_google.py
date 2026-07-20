"""Google Cloud Vision OCR for TikTok video frames."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from tiktok.enrichment.frames_smart import encode_frame_jpeg, sample_keyframes
from tiktok.enrichment.ocr_postprocess import clean_ocr_text, primary_source_type

logger = logging.getLogger(__name__)


def _get_vision_client():
    try:
        from google.cloud import vision
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-vision not installed. "
            "pip install -r requirements-enrichment.txt"
        ) from e

    from tiktok.enrichment.bigquery_loader import vision_enabled

    if not vision_enabled():
        raise RuntimeError(
            "VISION_ENABLED is false. Set VISION_ENABLED=true in server .env "
            "and ensure Vision API is enabled on cfme-mediaengagment-prod."
        )

    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS not set on comm-cme-p01. "
            "Example: /home/cme-user1/keys/tiktok-enrichment-worker.json "
            "(project cfme-mediaengagment-prod)."
        )
    if not os.path.isfile(creds):
        raise RuntimeError(f"Credentials file not found: {creds}")

    return vision.ImageAnnotatorClient()


def _video_duration_seconds(video_path: str) -> Optional[float]:
    try:
        import cv2

        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0) or 0.0
        total = float(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        cap.release()
        if fps > 0 and total > 0:
            return total / fps
    except Exception:
        return None
    return None


def _text_from_pages(annotation) -> str:
    """Rebuild text from Vision blocks/paragraphs to preserve reading order."""
    if not annotation or not annotation.pages:
        return (annotation.text or "").strip() if annotation else ""
    pages_out: List[str] = []
    for page in annotation.pages:
        blocks_out: List[str] = []
        for block in page.blocks or []:
            paras: List[str] = []
            for para in block.paragraphs or []:
                words: List[str] = []
                for word in para.words or []:
                    chars = "".join(s.text or "" for s in (word.symbols or []))
                    if chars:
                        words.append(chars)
                if words:
                    paras.append(" ".join(words))
            if paras:
                blocks_out.append("\n".join(paras))
        if blocks_out:
            pages_out.append("\n\n".join(blocks_out))
    rebuilt = "\n\n".join(pages_out).strip()
    return rebuilt or (annotation.text or "").strip()


def _languages_from_annotation(annotation) -> str:
    if not annotation or not annotation.pages:
        return ""
    counts: Dict[str, float] = {}
    for page in annotation.pages:
        props = getattr(page, "property", None) or getattr(page, "property_", None)
        langs = getattr(props, "detected_languages", None) if props else None
        if not langs:
            continue
        for lang in langs:
            code = getattr(lang, "language_code", "") or ""
            conf = float(getattr(lang, "confidence", 0) or 0)
            if code:
                counts[code] = counts.get(code, 0.0) + conf
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda x: -x[1])[0][0]


def ocr_image_bytes(image_bytes: bytes, client=None) -> Dict[str, Any]:
    """Run DOCUMENT_TEXT_DETECTION on one JPEG/PNG (retries transient API errors)."""
    from google.cloud import vision

    from tiktok.enrichment.retry import with_retries

    client = client or _get_vision_client()
    image = vision.Image(content=image_bytes)

    def _call():
        response = client.document_text_detection(image=image)
        if response.error.message:
            raise RuntimeError(response.error.message)
        return response

    response = with_retries(_call, attempts=3, label="vision_ocr")

    text = ""
    confidence: Optional[float] = None
    language = ""
    if response.full_text_annotation:
        text = _text_from_pages(response.full_text_annotation)
        language = _languages_from_annotation(response.full_text_annotation)
        confs: List[float] = []
        for page in response.full_text_annotation.pages or []:
            if page.confidence:
                confs.append(float(page.confidence))
        if confs:
            confidence = sum(confs) / len(confs)
    elif response.text_annotations:
        text = (response.text_annotations[0].description or "").strip()

    return {
        "ocr_text": text,
        "confidence": confidence,
        "language": language,
        "source": "google_vision",
    }


def ocr_video_file(
    video_path: str,
    *,
    max_frames: int = 12,
    client=None,
) -> Dict[str, Any]:
    """Sample keyframes and OCR each with Google Vision.

    Returns ``{"rows": [...], "stats": {...}}``. Empty-text frames are omitted
    from rows but counted in ``number_of_frames_processed``. Near-duplicate
    frames (repeated screenshots) are removed after cleaning.
    """
    from tiktok.enrichment.ocr_postprocess import dedupe_ocr_frames

    client = client or _get_vision_client()
    duration = _video_duration_seconds(video_path)
    frames = sample_keyframes(video_path, max_frames=max_frames, prefer_edges=True)
    rows: List[Dict[str, Any]] = []
    lang_votes: Dict[str, int] = {}
    frames_attempted = 0
    for frame_number, ts, frame in frames:
        frames_attempted += 1
        try:
            jpeg = encode_frame_jpeg(frame)
            result = ocr_image_bytes(jpeg, client=client)
        except Exception as e:
            logger.warning("Vision OCR failed frame=%s: %s", frame_number, e)
            continue
        raw = (result.get("ocr_text") or "").strip()
        lang = (result.get("language") or "").strip()
        if lang:
            lang_votes[lang] = lang_votes.get(lang, 0) + 1
        if not raw:
            continue
        cleaned = clean_ocr_text(raw)
        pct = None
        if duration and duration > 0:
            pct = round(min(100.0, max(0.0, 100.0 * float(ts) / duration)), 2)
        rows.append(
            {
                "frame_number": int(frame_number),
                "frame_timestamp": float(ts),
                "frame_pct": pct,
                "ocr_text": cleaned,
                "ocr_text_raw": raw,
                "ocr_text_clean": cleaned,
                "confidence": result.get("confidence"),
                "language": lang,
                "source": "google_vision",
                "source_type": primary_source_type(cleaned or raw),
            }
        )
    deduped = dedupe_ocr_frames(rows)
    confs = [r["confidence"] for r in deduped if r.get("confidence") is not None]
    avg_chars = (
        round(sum(len(r.get("ocr_text") or "") for r in deduped) / len(deduped), 2)
        if deduped
        else 0.0
    )
    ocr_language = ""
    if lang_votes:
        ocr_language = sorted(lang_votes.items(), key=lambda x: -x[1])[0][0]
    stats = {
        "number_of_frames_processed": frames_attempted,
        "frames_with_text": len(deduped),
        "average_text_per_frame": avg_chars,
        "ocr_confidence_avg": (sum(confs) / len(confs)) if confs else None,
        "ocr_language": ocr_language,
        "duration_seconds": duration,
    }
    return {"rows": deduped, "stats": stats}
