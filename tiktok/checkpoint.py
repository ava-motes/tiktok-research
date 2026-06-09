"""Checkpoint tracking for safe restarts of long-running pulls."""

import json
import os
import logging
from typing import Set

logger = logging.getLogger(__name__)


class CheckpointStore:
    """Tracks completed (handle, date_chunk) pairs for safe restarts.

    Keys are "handle|chunk_start|chunk_end" for video pulls,
    or just "handle" for user info pulls.
    Flushed to disk after each completed unit.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._completed: Set[str] = set()
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath) as f:
                data = json.load(f)
            self._completed = set(data.get("completed", []))
            logger.debug(f"Loaded {len(self._completed)} checkpoint entries from {self.filepath}")

    def _save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump({"completed": sorted(self._completed)}, f, indent=2)

    def make_key(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> str:
        if chunk_start:
            return f"{handle}|{chunk_start}|{chunk_end}"
        return handle

    def is_done(self, handle: str, chunk_start: str = "", chunk_end: str = "") -> bool:
        return self.make_key(handle, chunk_start, chunk_end) in self._completed

    def mark_done(self, handle: str, chunk_start: str = "", chunk_end: str = ""):
        key = self.make_key(handle, chunk_start, chunk_end)
        self._completed.add(key)
        self._save()

    def reset(self):
        self._completed.clear()
        self._save()
        logger.info(f"Checkpoints cleared: {self.filepath}")
