"""On-screen text (OCR) extraction for small-batch video evaluation."""

from tiktok.ocr.frames import iter_frames_at_interval, iter_frames_one_per_second
from tiktok.ocr.pipeline import OCRFrameResult, aggregate_ocr_for_video, extract_onscreen_text

__all__ = [
    "OCRFrameResult",
    "aggregate_ocr_for_video",
    "extract_onscreen_text",
    "iter_frames_at_interval",
    "iter_frames_one_per_second",
]
