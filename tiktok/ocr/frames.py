"""Sample video frames at a configurable rate (lightweight; no scene detection)."""

import logging
import os
from typing import Any, Generator, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import cv2
except ImportError:
    cv2 = None


def iter_frames_at_interval(
    video_path: str,
    max_frames: Optional[int] = None,
    seconds_between_samples: float = 1.0,
) -> Generator[Tuple[int, Any], None, None]:
    """Yield (frame_index, bgr_frame) every ``seconds_between_samples`` seconds.

    ``interval_frames = round(fps * seconds_between_samples)`` (at least 1).
    Examples with ~30 fps source:

    * ``seconds_between_samples=1.0`` → ~1 sample/sec (default TikTok UI pacing).
    * ``seconds_between_samples=2.0`` → ~0.5 sample/sec (denser in *time*, fewer
      frames; helps long flashes but can miss sub-second overlays).
    * ``seconds_between_samples=0.5`` → ~2 samples/sec (more work; better for
      short-lived on-screen text).

    Uses OpenCV CAP_PROP_FPS; falls back to 30 fps if unknown.
    """
    if cv2 is None:
        raise RuntimeError("opencv-python-headless is required. pip install opencv-python-headless")

    if not os.path.isfile(video_path):
        raise FileNotFoundError(video_path)

    if seconds_between_samples <= 0:
        raise ValueError("seconds_between_samples must be positive")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {video_path}")

    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    if fps <= 0:
        fps = 30.0
    interval = max(1, int(round(fps * seconds_between_samples)))

    idx = 0
    yielded = 0
    try:
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            if idx % interval == 0:
                yield idx, frame
                yielded += 1
                if max_frames is not None and yielded >= max_frames:
                    break
            idx += 1
    finally:
        cap.release()

    logger.debug(
        "Sampled %s frames from %s (interval_frames=%s, ~every %.2fs)",
        yielded,
        video_path,
        interval,
        interval / fps,
    )


def iter_frames_one_per_second(
    video_path: str,
    max_frames: Optional[int] = None,
) -> Generator[Tuple[int, Any], None, None]:
    """Backward-compatible alias: ~1 sample per second."""
    return iter_frames_at_interval(video_path, max_frames=max_frames, seconds_between_samples=1.0)
