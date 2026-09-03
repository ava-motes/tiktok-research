"""One-time Box OAuth login. Writes ``data/box_oauth.json`` (gitignored).

Run on your laptop after creating a Box custom app with redirect URI:

    http://127.0.0.1:8766/callback

    python scripts/box_oauth_login.py

Then copy ``data/box_oauth.json`` to the server (same path) and set
``BOX_CLIENT_ID`` / ``BOX_CLIENT_SECRET`` in the server ``.env``.
Daily collection uses the stored refresh token; it is rotated on each upload.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

from pathlib import Path
import importlib.util

def _setup_repo():
    for p in Path(__file__).resolve().parents:
        boot = p / "common" / "bootstrap.py"
        if boot.is_file():
            spec = importlib.util.spec_from_file_location("_tiktok_bootstrap", boot)
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            return mod.setup()
    raise RuntimeError("common/bootstrap.py not found")

ROOT = _setup_repo()

REDIRECT_URI = "http://127.0.0.1:8766/callback"
AUTHORIZE_URL = os.environ.get("BOX_AUTHORIZE_URL") or (
    "https://account.box.com/api/oauth2/authorize"
)


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv()
    client_id = (os.environ.get("BOX_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("BOX_CLIENT_SECRET") or "").strip()
    if not (client_id and client_secret):
        print(
            "Set BOX_CLIENT_ID and BOX_CLIENT_SECRET in .env first.",
            file=sys.stderr,
        )
        return 2

    params = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
        }
    )
    auth_url = f"{AUTHORIZE_URL}?{params}"
    got: dict = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return
            q = urllib.parse.parse_qs(parsed.query)
            got["code"] = (q.get("code") or [""])[0]
            got["error"] = (q.get("error") or [""])[0]
            body = (
                b"Box login complete. You can close this tab."
                if got["code"]
                else b"Box login failed. Return to the terminal."
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    httpd = HTTPServer(("127.0.0.1", 8766), Handler)
    print("Open this URL and approve Box access:\n")
    print(auth_url)
    print()
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass
    httpd.serve_forever()
    httpd.server_close()

    if got.get("error") or not got.get("code"):
        print(f"Box OAuth failed: {got.get('error') or 'no code'}", file=sys.stderr)
        return 1

    import requests

    from tiktok.box_delivery import BOX_TOKEN_URL, _oauth_store_path, _write_oauth_store

    resp = requests.post(
        BOX_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": got["code"],
            "redirect_uri": REDIRECT_URI,
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json() or {}
    refresh = (body.get("refresh_token") or "").strip()
    if not refresh:
        print("Box did not return a refresh token.", file=sys.stderr)
        return 1
    _write_oauth_store({"refresh_token": refresh})
    path = _oauth_store_path()
    print(f"Saved Box refresh token to {path}")
    print("Copy that file to comm-cme-p01:~/tiktok_research/data/box_oauth.json")
    print("Do not commit it. Do not paste the token into chat.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
