"""OCR pipeline: frame → text with deduplication and confidence logging (EasyOCR)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None

from tiktok.ocr.frames import iter_frames_at_interval

# EasyOCR confidence is 0–1; treat as 0–100 for logging parity with prior Tesseract scale
_MIN_CONFIDENCE = 30
_MIN_LINE_LEN = 2
_MAX_LINE_LEN = 500

_easyocr_reader = None


def _get_easyocr_reader():
    """Lazy-load EasyOCR reader (downloads models on first use)."""
    global _easyocr_reader
    if _easyocr_reader is None:
        try:
            import easyocr
        except ImportError as e:
            raise RuntimeError(
                "Install EasyOCR: pip install -r requirements-ocr.txt"
            ) from e
        logger.info("Initializing EasyOCR reader (first run may download models)")
        _easyocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    return _easyocr_reader


@dataclass
class OCRFrameResult:
    frame_index: int
    text_lines: List[str]
    mean_confidence: Optional[float]
    raw_line_count: int
    timestamp_sec: Optional[float] = None


@dataclass
class OCRVideoResult:
    video_id: str
    onscreen_text: str
    onscreen_text_raw: str
    frame_results: List[OCRFrameResult] = field(default_factory=list)
    mean_confidence_overall: Optional[float] = None
    frames_sampled: int = 0
    video_fps: float = 30.0
    seconds_between_samples: float = 1.0


def _clean_line(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _is_noise(line: str) -> bool:
    if len(line) < _MIN_LINE_LEN or len(line) > _MAX_LINE_LEN:
        return True
    if re.fullmatch(r"[\W_]+", line):
        return True
    return False


def _ocr_single_frame_bgr(frame_bgr: np.ndarray) -> Tuple[List[str], Optional[float]]:
    """Return (text_lines, mean EasyOCR confidence for kept detections, or None)."""
    if cv2 is None or np is None:
        raise RuntimeError("opencv-python-headless and numpy are required.")

    reader = _get_easyocr_reader()
    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    detections = reader.readtext(rgb)

    lines_out: List[str] = []
    confs: List[float] = []

    for _bbox, text, conf in detections:
        try:
            conf_pct = float(conf) * 100.0
        except (TypeError, ValueError):
            continue
        if conf_pct < _MIN_CONFIDENCE:
            continue
        ln = _clean_line(text)
        if _is_noise(ln):
            continue
        lines_out.append(ln)
        confs.append(conf_pct)

    frame_mean_conf = sum(confs) / len(confs) if confs else None

    if not lines_out and detections:
        logger.debug(
            "Frame OCR: %s raw detections, none passed filters",
            len(detections),
        )

    return lines_out, frame_mean_conf


def _read_fps(video_path: str) -> float:
    if cv2 is None:
        return 30.0
    cap = cv2.VideoCapture(video_path)
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    cap.release()
    return fps if fps > 0 else 30.0


def extract_onscreen_text(
    video_path: str,
    video_id: str,
    max_frames: Optional[int] = None,
    seconds_between_samples: float = 1.0,
) -> OCRVideoResult:
    """Run OCR over sampled frames; produce deduped string plus raw (order-preserving) text."""
    vfps = _read_fps(video_path)
    frame_results: List[OCRFrameResult] = []
    all_confs: List[float] = []
    ordered_unique_lines: List[str] = []
    seen_globally = set()
    raw_frame_blocks: List[str] = []

    for frame_idx, frame in iter_frames_at_interval(
        video_path,
        max_frames=max_frames,
        seconds_between_samples=seconds_between_samples,
    ):
        ts = frame_idx / vfps if vfps else None
        try:
            lines, frame_mean_conf = _ocr_single_frame_bgr(frame)
        except Exception as e:
            logger.warning("OCR failed at frame %s: %s", frame_idx, e)
            frame_results.append(
                OCRFrameResult(frame_idx, [], None, 0, timestamp_sec=ts),
            )
            continue

        if frame_mean_conf is not None:
            all_confs.append(frame_mean_conf)

        raw_frame_blocks.append("\n".join(lines))

        for ln in lines:
            k = ln.lower()
            if k not in seen_globally and not _is_noise(ln):
                seen_globally.add(k)
                ordered_unique_lines.append(ln)

        frame_results.append(
            OCRFrameResult(
                frame_index=frame_idx,
                text_lines=lines,
                mean_confidence=frame_mean_conf,
                raw_line_count=len(lines),
                timestamp_sec=ts,
            ),
        )

    onscreen = "\n".join(ordered_unique_lines)
    onscreen_raw = "\n\n".join(raw_frame_blocks)
    overall = sum(all_confs) / len(all_confs) if all_confs else None

    return OCRVideoResult(
        video_id=video_id,
        onscreen_text=onscreen,
        onscreen_text_raw=onscreen_raw,
        frame_results=frame_results,
        mean_confidence_overall=overall,
        frames_sampled=len(frame_results),
        video_fps=vfps,
        seconds_between_samples=seconds_between_samples,
    )


def aggregate_ocr_for_video(video_path: str, video_id: str, **kwargs) -> OCRVideoResult:
    """Alias for :func:`extract_onscreen_text`."""
    return extract_onscreen_text(video_path, video_id, **kwargs)
