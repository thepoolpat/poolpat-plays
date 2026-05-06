"""Spotify OAuth 2.0 Authorization Code Flow with PKCE.

Run this script once locally to obtain a refresh token:
    python pipeline/spotify_auth.py

The refresh token should be stored as a GitHub Actions secret
(SPOTIFY_REFRESH_TOKEN) for use by the daily pipeline.
"""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
import subprocess
import sys
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import requests

TOKEN_URL = "https://accounts.spotify.com/api/token"
AUTH_URL = "https://accounts.spotify.com/authorize"
DEFAULT_REDIRECT_URI = "http://127.0.0.1:8888/callback"
DEFAULT_SCOPES = "user-read-private user-top-read user-read-recently-played streaming"
SHARED_TOKEN_REPOS = ["thepoolpat/poolpat-plays", "thepoolpat/poolpat-portfolio"]


def generate_pkce_pair() -> tuple[str, str]:
    """Generate a PKCE code verifier and its SHA-256 challenge."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def build_auth_url(
    client_id: str,
    redirect_uri: str,
    scopes: str,
    code_challenge: str,
) -> str:
    """Build the Spotify authorization URL with PKCE parameters."""
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scopes,
        "code_challenge_method": "S256",
        "code_challenge": code_challenge,
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code(
    client_id: str,
    code: str,
    code_verifier: str,
    redirect_uri: str,
) -> dict[str, Any]:
    """Exchange an authorization code for access + refresh tokens."""
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": code_verifier,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def refresh_access_token(client_id: str, refresh_token: str) -> dict[str, Any]:
    """Use a refresh token to obtain a new access token.

    Returns the full token response. If Spotify rotated the refresh token,
    the response will contain a new 'refresh_token' field.
    """
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "refresh_token" in data and data["refresh_token"] != refresh_token:
        print(
            "\n⚠  Spotify rotated your refresh token. Update SPOTIFY_REFRESH_TOKEN secret.",
            file=sys.stderr,
        )
        if gh_env := os.environ.get("GITHUB_ENV"):
            new_token = data["refresh_token"]
            # Register as masked BEFORE writing to GITHUB_ENV — otherwise the
            # token leaks into subsequent steps' env block in the runner log.
            print(f"::add-mask::{new_token}")
            try:
                with open(gh_env, "a") as f:
                    f.write(f"SPOTIFY_REFRESH_TOKEN={new_token}\n")
            except OSError as e:
                print(f"  ⚠ Could not write to $GITHUB_ENV: {e}", file=sys.stderr)
        new_refresh = data["refresh_token"]
        for repo in SHARED_TOKEN_REPOS:
            try:
                subprocess.run(
                    ["gh", "secret", "set", "SPOTIFY_REFRESH_TOKEN",
                     "--repo", repo, "--body", new_refresh],
                    check=True, capture_output=True,
                )
                print(f"  ✅ {repo} SPOTIFY_REFRESH_TOKEN updated")
            except Exception as e:
                print(f"  ⚠ Could not update {repo}: {e}", file=sys.stderr)

    return data


def _run_local_auth(client_id: str, redirect_uri: str, scopes: str) -> str:
    """Run the full local OAuth flow: open browser, wait for callback, return auth code."""
    verifier, challenge = generate_pkce_pair()
    auth_url = build_auth_url(client_id, redirect_uri, scopes, challenge)

    received_code: list[str] = []

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)

            if "error" in params:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f"Error: {params['error'][0]}".encode())
                return

            code = params.get("code", [None])[0]
            if code:
                received_code.append(code)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Authorization successful! You can close this tab.")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"No authorization code received.")

        def log_message(self, format, *args):
            pass

    parsed = urllib.parse.urlparse(redirect_uri)
    port = parsed.port or 8888

    print(f"\nOpening browser for Spotify authorization...")
    print(f"If the browser doesn't open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    # Try local callback server first; fall back to manual paste
    try:
        server = HTTPServer(("127.0.0.1", port), CallbackHandler)
        server.timeout = 120
        server.handle_request()
        server.server_close()
    except OSError:
        print(f"Could not start local server on port {port}.")
        print("After authorizing in the browser, paste the full callback URL here.")
        print("It will look like: http://localhost:8888/callback?code=AQD...\n")
        callback_url = input("Callback URL: ").strip()
        query = urllib.parse.urlparse(callback_url).query
        params = urllib.parse.parse_qs(query)
        code = params.get("code", [None])[0]
        if code:
            received_code.append(code)

    if not received_code:
        print("No authorization code received. Timed out or denied.", file=sys.stderr)
        sys.exit(1)

    tokens = exchange_code(client_id, received_code[0], verifier, redirect_uri)
    return tokens


if __name__ == "__main__":
    client_id = os.environ.get("SPOTIFY_CLIENT_ID") or input("Spotify Client ID: ").strip()
    redirect_uri = os.environ.get("SPOTIFY_REDIRECT_URI", DEFAULT_REDIRECT_URI)
    scopes = os.environ.get("SPOTIFY_SCOPES", DEFAULT_SCOPES)

    if not client_id:
        print("SPOTIFY_CLIENT_ID is required.", file=sys.stderr)
        sys.exit(1)

    tokens = _run_local_auth(client_id, redirect_uri, scopes)

    refresh = tokens["refresh_token"]
    print("\n" + "=" * 60)
    print("Authorization successful!")
    print("=" * 60)
    print(f"\nScopes granted: {tokens.get('scope', 'unknown')}")
    print(f"Expires in: {tokens.get('expires_in', '?')}s")

    try:
        import subprocess
        subprocess.run(["pbcopy"], input=refresh.encode(), check=True)
        print("\nRefresh token copied to clipboard.")
    except Exception:
        print(f"\nRefresh token:\n  {refresh}")

    print("\nNext steps:")
    print("  1. Go to your repo Settings → Secrets → Actions")
    print("  2. Add/update secret: SPOTIFY_REFRESH_TOKEN")
    print("  3. Paste the value from your clipboard")
    print("=" * 60)
