"""Modular Spotify Web API client with typed responses and retry logic."""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spotify_errors import (
    SpotifyAuthError,
    SpotifyRateLimitError,
    raise_for_status,
)

API_BASE = "https://api.spotify.com/v1"
MAX_RETRIES = 3
BATCH_TRACKS = 50
BATCH_ALBUMS = 20


class SpotifyClient:
    def __init__(
        self,
        access_token: str,
        client_id: str | None = None,
        refresh_token: str | None = None,
    ):
        self._token = access_token
        self._client_id = client_id
        self._refresh_token = refresh_token
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        })

    def _refresh(self) -> None:
        if not self._client_id or not self._refresh_token:
            raise SpotifyAuthError(401, "No refresh credentials available")

        from spotify_auth import refresh_access_token

        tokens = refresh_access_token(self._client_id, self._refresh_token)
        self._token = tokens["access_token"]
        self._session.headers["Authorization"] = f"Bearer {self._token}"

        if "refresh_token" in tokens:
            self._refresh_token = tokens["refresh_token"]

    def _request(self, method: str, endpoint: str, **kwargs) -> dict | None:
        url = f"{API_BASE}{endpoint}" if endpoint.startswith("/") else endpoint
        refreshed = False

        for attempt in range(MAX_RETRIES + 1):
            resp = self._session.request(method, url, timeout=30, **kwargs)

            if resp.status_code == 204:
                return None

            try:
                raise_for_status(resp)
                return resp.json()
            except SpotifyAuthError:
                if not refreshed:
                    self._refresh()
                    refreshed = True
                    continue
                raise
            except SpotifyRateLimitError as e:
                if attempt < MAX_RETRIES:
                    wait = min(e.retry_after, 30)
                    print(f"Rate limited, waiting {wait}s...", file=sys.stderr)
                    time.sleep(wait)
                    continue
                raise

        return resp.json()

    def _paginate(self, endpoint: str, params: dict | None = None, limit: int = 50) -> list[dict]:
        results: list[dict] = []
        p = dict(params or {})
        p.setdefault("limit", min(limit, 50))
        url = f"{API_BASE}{endpoint}"

        while url and len(results) < limit:
            data = self._request("GET", url, params=p if API_BASE in url else None)
            if not data:
                break

            items = data.get("items", [])
            if not items:
                break

            results.extend(items)
            url = data.get("next")
            p = {}

        return results[:limit]

    # ── User ──

    def get_current_user(self) -> dict:
        return self._request("GET", "/me")

    def get_top_tracks(self, time_range: str = "medium_term", limit: int = 50) -> list[dict]:
        return self._paginate("/me/top/tracks", {"time_range": time_range}, limit)

    def get_top_artists(self, time_range: str = "medium_term", limit: int = 50) -> list[dict]:
        return self._paginate("/me/top/artists", {"time_range": time_range}, limit)

    def get_recently_played(self, limit: int = 50) -> list[dict]:
        data = self._request("GET", "/me/player/recently-played", params={"limit": min(limit, 50)})
        return (data or {}).get("items", [])

    # ── Tracks ──

    def get_tracks(self, track_ids: list[str]) -> list[dict]:
        results: list[dict] = []
        for i in range(0, len(track_ids), BATCH_TRACKS):
            batch = track_ids[i : i + BATCH_TRACKS]
            data = self._request("GET", "/tracks", params={"ids": ",".join(batch)})
            results.extend((data or {}).get("tracks", []))
        return results

    def get_audio_features(self, track_ids: list[str]) -> list[dict]:
        results: list[dict] = []
        for i in range(0, len(track_ids), BATCH_TRACKS):
            batch = track_ids[i : i + BATCH_TRACKS]
            data = self._request("GET", "/audio-features", params={"ids": ",".join(batch)})
            results.extend((data or {}).get("audio_features", []))
        return results

    def search_tracks(self, query: str, limit: int = 20) -> list[dict]:
        data = self._request("GET", "/search", params={"q": query, "type": "track", "limit": limit})
        return (data or {}).get("tracks", {}).get("items", [])

    # ── Playlists ──

    def get_playlists(self, limit: int = 50) -> list[dict]:
        return self._paginate("/me/playlists", limit=limit)

    def get_playlist_tracks(self, playlist_id: str, limit: int = 100) -> list[dict]:
        return self._paginate(f"/playlists/{playlist_id}/tracks", limit=limit)

    # ── Playback (requires Premium + active device) ──

    def get_playback_state(self) -> dict | None:
        return self._request("GET", "/me/player")

    def play(self, context_uri: str | None = None, uris: list[str] | None = None) -> None:
        body: dict[str, Any] = {}
        if context_uri:
            body["context_uri"] = context_uri
        if uris:
            body["uris"] = uris
        self._request("PUT", "/me/player/play", json=body or None)

    def pause(self) -> None:
        self._request("PUT", "/me/player/pause")

    def skip_next(self) -> None:
        self._request("POST", "/me/player/next")

    def skip_previous(self) -> None:
        self._request("POST", "/me/player/previous")

    def set_volume(self, percent: int) -> None:
        self._request("PUT", "/me/player/volume", params={"volume_percent": max(0, min(100, percent))})
