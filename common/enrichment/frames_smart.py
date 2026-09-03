"""Intelligent frame sampling for OCR (quintiles + optional fill)."""

from __future__ import annotations

import logging
from typing import Any, List, Tuple

logger = logging.getLogger(__name__)

try:
    import cv2
except ImportError:
    cv2 = None

# Research default: begin / quarter / mid / three-quarter / end
DEFAULT_PERCENTILES = (0.0, 0.25, 0.50, 0.75, 1.0)
# Extra early samples — short-lived title cards / hooks often appear in first ~10s
# and are missed on long videos when spacing between frames is ~30s.
EARLY_SECONDS = (1.0, 3.0, 8.0)


def sample_keyframes(
    video_path: str,
    *,
    max_frames: int = 12,
    prefer_edges: bool = True,
    percentiles: Tuple[float, ...] = DEFAULT_PERCENTILES,
    early_seconds: Tuple[float, ...] = EARLY_SECONDS,
) -> List[Tuple[int, float, Any]]:
    """Return list of (frame_number, timestamp_sec, bgr_frame).

    Strategy (production OCR on comm-cme-p01):
    - Always sample at 0%, 25%, 50%, 75%, 100% of duration.
    - For longer videos, also sample early absolute times (1s / 3s / 8s) so
      short-lived opening overlays are not skipped.
    - Fill remaining slots (up to ``max_frames``, typically 8–12) evenly.
    - Caps at ``max_frames`` (recommended 8–12; do not OCR every frame).
    """
    if cv2 is None:
        raise RuntimeError(
            "opencv-python-headless is required for OCR frame sampling. "
            "pip install opencv-python-headless"
        )

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video_path}")

    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0) or 30.0
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if total <= 0:
            from tiktok.ocr.frames import iter_frames_at_interval

            out: List[Tuple[int, float, Any]] = []
            for idx, frame in iter_frames_at_interval(
                video_path, max_frames=max_frames, seconds_between_samples=1.0
            ):
                out.append((idx, idx / fps, frame))
            return out

        duration_s = total / fps if fps > 0 else 0.0
        indices: List[int] = []
        if prefer_edges:
            for pct in percentiles:
                # 100% → last frame
                if pct >= 1.0:
                    indices.append(max(0, total - 1))
                else:
                    indices.append(min(total - 1, max(0, int(total * pct))))

        # Front-load long videos: opening hooks / "where is…" cards often leave
        # before the next percentile sample.
        if duration_s >= 45.0:
            for sec in early_seconds:
                if 0 < sec < duration_s:
                    indices.append(min(total - 1, max(0, int(round(sec * fps)))))

        n_fill = max(0, max_frames - len(set(indices)))
        if n_fill > 0 and total > 1:
            step = max(1, total // (n_fill + 1))
            for i in range(1, n_fill + 1):
                indices.append(min(total - 1, i * step))

        # Prefer early + quintile frames if we exceed max_frames
        uniq = sorted(set(indices))
        if len(uniq) > max_frames:
            # Keep percentile anchors + early seconds, then fill from remaining
            priority: List[int] = []
            for pct in percentiles:
                if pct >= 1.0:
                    priority.append(max(0, total - 1))
                else:
                    priority.append(min(total - 1, max(0, int(total * pct))))
            if duration_s >= 45.0:
                for sec in early_seconds:
                    if 0 < sec < duration_s:
                        priority.append(min(total - 1, max(0, int(round(sec * fps)))))
            ordered: List[int] = []
            for fi in priority + uniq:
                if fi not in ordered:
                    ordered.append(fi)
                if len(ordered) >= max_frames:
                    break
            indices = ordered
        else:
            indices = uniq[:max_frames]

        results: List[Tuple[int, float, Any]] = []
        for fi in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, fi)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            results.append((fi, fi / fps, frame))
        logger.debug(
            "Sampled %s keyframes from %s (total=%s, percentiles=%s)",
            len(results),
            video_path,
            total,
            percentiles,
        )
        return results
    finally:
        cap.release()


def encode_frame_jpeg(frame: Any, quality: int = 85) -> bytes:
    """Encode OpenCV BGR frame to JPEG bytes for Vision API."""
    if cv2 is None:
        raise RuntimeError("opencv-python-headless is required")
    ok, buf = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    if not ok:
        raise RuntimeError("JPEG encode failed")
    return buf.tobytes()
