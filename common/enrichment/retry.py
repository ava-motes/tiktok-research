"""Exponential backoff for transient API failures."""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

TRANSIENT_MARKERS = (
    "timeout",
    "timed out",
    "temporarily unavailable",
    "rate limit",
    "429",
    "500",
    "502",
    "503",
    "504",
    "service unavailable",
    "connection reset",
    "connection aborted",
    "deadline exceeded",
    "unavailable",
)

# Do not retry these — looping burns the same exhausted quota.
PERMANENT_MARKERS = (
    "credit_balance",
    "insufficient_quota",
    "quota_exceeded",
    "billing_hard_limit",
)


def is_transient_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if any(m in msg for m in PERMANENT_MARKERS):
        return False
    return any(m in msg for m in TRANSIENT_MARKERS)


def with_retries(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 1.5,
    max_delay: float = 30.0,
    label: str = "op",
) -> T:
    """Call ``fn`` with exponential backoff on transient errors."""
    last: Optional[BaseException] = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — intentional boundary
            last = e
            if i >= attempts or not is_transient_error(e):
                raise
            delay = min(max_delay, base_delay * (2 ** (i - 1)))
            delay *= 0.8 + 0.4 * random.random()
            logger.warning(
                "%s transient failure attempt %s/%s: %s; sleep %.1fs",
                label,
                i,
                attempts,
                e,
                delay,
            )
            time.sleep(delay)
    assert last is not None
    raise last


def classify_failure(exc: BaseException) -> Tuple[str, bool]:
    """Return (reason, is_transient)."""
    transient = is_transient_error(exc)
    msg = str(exc)[:200]
    if transient:
        return f"transient:{msg}", True
    return f"permanent:{msg}", False
