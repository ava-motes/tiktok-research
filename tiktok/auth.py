"""OAuth 2.0 client credentials flow for TikTok Research API."""

import time
import logging
import requests

logger = logging.getLogger(__name__)

_token_cache = {"access_token": None, "expires_at": 0}

# Module-level config — set by init() before first use
_base_url = None
_client_key = None
_client_secret = None


def init(base_url: str, client_key: str, client_secret: str):
    """Initialize auth module with credentials from config."""
    global _base_url, _client_key, _client_secret
    _base_url = base_url
    _client_key = client_key
    _client_secret = client_secret


def get_access_token() -> str:
    """Obtain an access token using OAuth 2.0 client credentials flow."""
    if not _base_url:
        raise RuntimeError("Call tiktok.auth.init() before using auth functions")

    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    logger.debug("Requesting new OAuth token")
    resp = requests.post(
        f"{_base_url}/oauth/token/",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_key": _client_key,
            "client_secret": _client_secret,
            "grant_type": "client_credentials",
        },
    )
    resp.raise_for_status()
    data = resp.json()

    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 7200)
    logger.debug("OAuth token refreshed")

    return _token_cache["access_token"]


def auth_headers() -> dict:
    """Return headers dict with a valid Bearer token."""
    return {"Authorization": f"Bearer {get_access_token()}"}
