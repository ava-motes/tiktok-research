"""Modular enrichment workers: transcription, Google Vision OCR, emoji, BigQuery sync.

Does not modify TikTok Research API collection. Writes to local SQLite staging tables
and optionally syncs to BigQuery when credentials are configured.
"""

from .worker_log import WorkerResult, log_worker_result

__all__ = ["WorkerResult", "log_worker_result"]
