"""Structured logging helpers for enrichment workers."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class WorkerResult:
    worker: str
    video_id: str
    ok: bool
    started_at: str
    ended_at: str
    elapsed_seconds: float
    error: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkerTimer:
    """Context manager that builds a WorkerResult and logs success/failure."""

    def __init__(self, worker: str, video_id: str):
        self.worker = worker
        self.video_id = video_id
        self.started_at = utc_now_iso()
        self._t0 = time.perf_counter()
        self.ok = False
        self.error: Optional[str] = None
        self.detail: Dict[str, Any] = {}

    def __enter__(self) -> "WorkerTimer":
        logger.info("[%s] start video_id=%s at=%s", self.worker, self.video_id, self.started_at)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        ended = utc_now_iso()
        elapsed = time.perf_counter() - self._t0
        if exc is not None:
            self.ok = False
            self.error = f"{type(exc).__name__}: {exc}"
        result = WorkerResult(
            worker=self.worker,
            video_id=self.video_id,
            ok=self.ok,
            started_at=self.started_at,
            ended_at=ended,
            elapsed_seconds=round(elapsed, 3),
            error=self.error,
            detail=self.detail,
        )
        log_worker_result(result)
        # Swallow exceptions so one video failure never stops the batch.
        return True

    def success(self, **detail: Any) -> None:
        self.ok = True
        self.detail.update(detail)

    def fail(self, reason: str, **detail: Any) -> None:
        self.ok = False
        self.error = reason
        self.detail.update(detail)

    def to_result(self) -> WorkerResult:
        ended = utc_now_iso()
        elapsed = time.perf_counter() - self._t0
        return WorkerResult(
            worker=self.worker,
            video_id=self.video_id,
            ok=self.ok,
            started_at=self.started_at,
            ended_at=ended,
            elapsed_seconds=round(elapsed, 3),
            error=self.error,
            detail=self.detail,
        )


def log_worker_result(result: WorkerResult) -> None:
    if result.ok:
        logger.info(
            "[%s] SUCCESS video_id=%s elapsed=%.2fs detail=%s",
            result.worker,
            result.video_id,
            result.elapsed_seconds,
            result.detail,
        )
    else:
        logger.error(
            "[%s] FAIL video_id=%s elapsed=%.2fs error=%s detail=%s",
            result.worker,
            result.video_id,
            result.elapsed_seconds,
            result.error,
            result.detail,
        )
