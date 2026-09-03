"""Checkpoint tracking for safe restarts of long-running pulls."""

import json
import os
import logging
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)


class CheckpointStore:
    """Tracks completed (handle, date_chunk) pairs for safe restarts.

    Keys are "handle|chunk_start|chunk_end" for video pulls,
    or just "handle" for user info pulls.
    Flushed to disk after each completed unit.

    ``completed`` = successful query (including zero videos).
    ``failed`` = exhausted retries with no usable pages. Resume with
    ``clear_failed()`` to retry them.
    ``partial`` = some pages persisted, then a persistent HTTP 500 (or
    similar) stopped pagination. Not settled: the next run resumes from
    the saved cursor and does not re-fetch persisted pages. Pipeline 1/2
    never write this key.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._completed: Set[str] = set()
        self._failed: Set[str] = set()
        self._partial: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath) as f:
                data = json.load(f)
            self._completed = set(data.get("completed", []))
            self._failed = set(data.get("failed", []))
            raw_partial = data.get("partial") or {}
            self._partial = {}
            if isinstance(raw_partial, dict):
                for key, meta in raw_partial.items():
                    if isinstance(meta, dict):
                        self._partial[str(key)] = dict(meta)
            logger.debug(
                "Loaded %s completed / %s failed / %s partial checkpoint entries from %s",
                len(self._completed),
                len(self._failed),
                len(self._partial),
                self.filepath,
            )

    def _save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        payload: Dict[str, Any] = {
            "completed": sorted(self._completed),
            "failed": sorted(self._failed),
        }
        # Omit empty partial so Pipeline 1/2 files stay {completed, failed}.
        if self._partial:
            payload["partial"] = {
                key: dict(meta) for key, meta in sorted(self._partial.items())
            }
        with open(self.filepath, "w") as f:
            json.dump(payload, f, indent=2)

    def make_key(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> str:
        if chunk_start:
            return f"{handle}|{chunk_start}|{chunk_end}"
        return handle

    def is_done(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> bool:
        return self.make_key(handle, chunk_start, chunk_end) in self._completed

    def is_failed(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> bool:
        return self.make_key(handle, chunk_start, chunk_end) in self._failed

    def is_partial(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> bool:
        return self.make_key(handle, chunk_start, chunk_end) in self._partial

    def is_settled(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> bool:
        """True when this chunk should not be queried again unless failed is cleared.

        Partial keywords stay pending so the next run can resume from cursor.
        """
        return self.is_done(handle, chunk_start, chunk_end) or self.is_failed(
            handle, chunk_start, chunk_end
        )

    def get_partial(
        self, handle: str, chunk_start: str = "", chunk_end: str = ""
    ) -> Optional[Dict[str, Any]]:
        key = self.make_key(handle, chunk_start, chunk_end)
        meta = self._partial.get(key)
        return dict(meta) if meta else None

    def mark_done(self, handle: str, chunk_start: str = "", chunk_end: str = ""):
        key = self.make_key(handle, chunk_start, chunk_end)
        self._completed.add(key)
        self._failed.discard(key)
        self._partial.pop(key, None)
        self._save()

    def mark_failed(self, handle: str, chunk_start: str = "", chunk_end: str = ""):
        key = self.make_key(handle, chunk_start, chunk_end)
        self._failed.add(key)
        self._completed.discard(key)
        self._partial.pop(key, None)
        self._save()
        logger.warning("Checkpointed as failed (will retry on --retry-failed): %s", key)

    def mark_partial(
        self,
        handle: str,
        chunk_start: str = "",
        chunk_end: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ):
        key = self.make_key(handle, chunk_start, chunk_end)
        payload = dict(meta or {})
        payload["status"] = "partial"
        self._partial[key] = payload
        self._completed.discard(key)
        self._failed.discard(key)
        self._save()
        logger.warning(
            "Checkpointed as partial (resume from cursor=%s page=%s): %s",
            payload.get("cursor"),
            payload.get("page"),
            key,
        )

    def clear_failed(self):
        if not self._failed:
            return
        n = len(self._failed)
        self._failed.clear()
        self._save()
        logger.info("Cleared %s failed checkpoint entries for retry: %s", n, self.filepath)

    def reset(self):
        self._completed.clear()
        self._failed.clear()
        self._partial.clear()
        self._save()
        logger.info(f"Checkpoints cleared: {self.filepath}")
