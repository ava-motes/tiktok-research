"""Rate-limited HTTP wrapper for TikTok API with raw response streaming."""

import json
import os
import time
import logging
from datetime import datetime, timezone

import requests

from tiktok.auth import auth_headers

logger = logging.getLogger(__name__)


class RawResponseWriter:
    """Appends raw API responses to handle-specific JSONL files."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def write(self, endpoint: str, handle: str, request_body: dict,
              http_status: int, response_body: dict, **extra):
        category = "videos" if "video" in endpoint else "users"
        dirpath = os.path.join(self.base_dir, category)
        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, f"{handle}.jsonl")

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint,
            "handle": handle,
            "http_status": http_status,
            "request": request_body,
            "response": response_body,
            **extra,
        }

        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


class TikTokClient:
    """HTTP client with rate-limit handling and raw response archiving."""

    def __init__(self, base_url: str, raw_dir: str, db_conn=None):
        self.base_url = base_url
        self.writer = RawResponseWriter(raw_dir)
        self.db_conn = db_conn

    def post(self, endpoint: str, body: dict, params: dict,
             handle: str = "", **writer_extra) -> dict:
        """Make a POST request with rate-limit retry and response archiving.

        Returns the parsed JSON response body, or None on non-retryable errors.
        """
        url = f"{self.base_url}/{endpoint}"

        while True:
            try:
                resp = requests.post(
                    url,
                    headers={**auth_headers(), "Content-Type": "application/json"},
                    json=body,
                    params=params,
                    timeout=30,
                )
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.error(f"Request failed for @{handle} ({type(e).__name__}) — skipping")
                return None

            if resp.status_code == 429:
                try:
                    error_code = resp.json().get("error", {}).get("code", "")
                except ValueError:
                    error_code = ""
                if error_code == "daily_quota_limit_exceeded":
                    logger.error("Daily quota limit exceeded — stopping")
                    raise RuntimeError("daily_quota_limit_exceeded")
                retry_after = int(resp.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited — waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            try:
                response_body = resp.json()
            except ValueError:
                response_body = {"raw_text": resp.text}

            # Stream raw response to JSONL
            self.writer.write(
                endpoint=endpoint,
                handle=handle,
                request_body=body,
                http_status=resp.status_code,
                response_body=response_body,
                **writer_extra,
            )

            # Also store in SQLite if connection available
            if self.db_conn:
                from tiktok.db import insert_raw_response
                insert_raw_response(
                    self.db_conn, endpoint, handle, body, response_body, resp.status_code
                )
                self.db_conn.commit()

            if not resp.ok:
                logger.error(f"Error {resp.status_code} for @{handle}: {resp.text}")
                return None

            return response_body
