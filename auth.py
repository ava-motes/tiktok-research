import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

_token_cache = {"access_token": None, "expires_at": 0}

BASE_URL = "https://open.tiktokapis.com/v2"


def get_access_token():
    """Obtain an access token using OAuth 2.0 client credentials flow."""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    client_key = os.getenv("TIKTOK_CLIENT_KEY")
    client_secret = os.getenv("TIKTOK_CLIENT_SECRET")

    if not client_key or not client_secret:
        raise RuntimeError("TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET must be set in .env")

    resp = requests.post(
        f"{BASE_URL}/oauth/token/",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_key": client_key,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
    )
    resp.raise_for_status()
    data = resp.json()

    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 7200)

    return _token_cache["access_token"]


def auth_headers():
    """Return headers dict with a valid Bearer token."""
    return {"Authorization": f"Bearer {get_access_token()}"}
