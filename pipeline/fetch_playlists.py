"""
fetch_playlists.py

Discovers which Spotify playlists feature Poolpat tracks using:
  1. Spotify Web API — get artist tracks, then search playlists by track name
  2. Playlistcheck RapidAPI — enrich each playlist with follower count,
     curator contact, and historical data

Outputs: data/playlists.json

Requires env vars:
  SPOTIFY_CLIENT_ID, SPOTIFY_REFRESH_TOKEN
  RAPIDAPI_KEY

Note: Uses PKCE refresh flow (no client_secret) matching spotify_auth.py.

Playlistcheck API (RapidAPI):
  Only endpoint: GET https://playlistcheck.p.rapidapi.com/playlist
  Required param: playlist_id (Spotify playlist ID)
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ARTIST_ID = "4rr3o9anpUXitNXo0W4uX7"  # Poolpat
OUTPUT_PATH = Path("data/playlists.json")
PLAYLISTCHECK_URL = "https://playlistcheck.p.rapidapi.com/playlist"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"

# Both repos share the same Spotify refresh token — update both on rotation
SHARED_TOKEN_REPOS = [
    "thepoolpat/poolpat-plays",
    "thepoolpat/poolpat-portfolio",
]


# ---------------------------------------------------------------------------
# Spotify auth — PKCE refresh flow
# ---------------------------------------------------------------------------
def get_spotify_token() -> str:
    client_id = os.environ["SPOTIFY_CLIENT_ID"].strip()
    refresh_token = os.environ["SPOTIFY_REFRESH_TOKEN"].strip()

    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        },
        timeout=15,
    )

    if not resp.ok:
        raise RuntimeError(
            f"Spotify token refresh failed ({resp.status_code}): {resp.text}"
        )

    data = resp.json()

    # Auto-save rotated refresh token to ALL repos that share this token
    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("  ⚠ Spotify rotated refresh token — updating all repos...")
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

    return data["access_token"]


# ---------------------------------------------------------------------------
# Spotify helpers
# ---------------------------------------------------------------------------
def spotify_get(path: str, token: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{SPOTIFY_API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=15,
    )
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        print(f"  Spotify rate limit — sleeping {retry_after}s")
        time.sleep(retry_after)
        return spotify_get(path, token, params)
    resp.raise_for_status()
    return resp.json()


def get_artist_tracks(token: str) -> list[dict]:
    """Returns deduplicated list of {id, name} for all tracks by Poolpat.

    Uses only album,single groups — appears_on requires elevated user scopes
    and causes 400 errors with PKCE tokens.
    """
    tracks = []
    # appears_on excluded: requires user-library scope, causes 400 with PKCE token
    for group in ("album", "single"):
        try:
            resp = spotify_get(
                f"/artists/{ARTIST_ID}/albums",
                token,
                {"include_groups": group, "limit": 50, "market": "IE"},
            )
            for album in resp.get("items", []):
                try:
                    album_tracks = spotify_get(
                        f"/albums/{album['id']}/tracks", token, {"limit": 50}
                    )
                    for t in album_tracks.get("items", []):
                        if ARTIST_ID in [a["id"] for a in t.get("artists", [])]:
                            tracks.append({"id": t["id"], "name": t["name"]})
                except Exception as e:
                    print(f"  Skipping album {album['id']}: {e}")
        except Exception as e:
            print(f"  Skipping group '{group}': {e}")

    seen: set[str] = set()
    unique = []
    for t in tracks:
        if t["id"] not in seen:
            seen.add(t["id"])
            unique.append(t)
    return unique


def search_playlists_for_track(track_name: str, token: str) -> list[str]:
    """Search Spotify for playlists by track name. Returns playlist IDs."""
    try:
        results = spotify_get(
            "/search", token,
            {"q": track_name, "type": "playlist", "limit": 20, "market": "IE"},
        )
        items = results.get("playlists", {}).get("items") or []
        return [p["id"] for p in items if p and p.get("id")]
    except Exception as e:
        print(f"  Search failed for '{track_name}': {e}")
        return []


def search_playlists_for_artist(token: str) -> list[str]:
    """Search Spotify for playlists by artist name. Returns playlist IDs."""
    try:
        results = spotify_get(
            "/search", token,
            {"q": "Poolpat", "type": "playlist", "limit": 20, "market": "IE"},
        )
        items = results.get("playlists", {}).get("items") or []
        return [p["id"] for p in items if p and p.get("id")]
    except Exception as e:
        print(f"  Artist playlist search failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Playlistcheck RapidAPI
# ---------------------------------------------------------------------------
def enrich_playlist(playlist_id: str) -> dict | None:
    """Fetch enriched playlist data from Playlistcheck. Returns None on any error."""
    try:
        resp = requests.get(
            PLAYLISTCHECK_URL,
            headers={
                "x-rapidapi-host": "playlistcheck.p.rapidapi.com",
                "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
            },
            params={"playlist_id": playlist_id},
            timeout=20,
        )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            print(f"  Rate limit — sleeping {retry_after}s")
            time.sleep(retry_after)
            return enrich_playlist(playlist_id)
        if resp.status_code in (404, 422):
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Playlistcheck error for {playlist_id}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not os.environ.get("RAPIDAPI_KEY"):
        print("WARNING: RAPIDAPI_KEY not set — skipping playlist fetch")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not OUTPUT_PATH.exists():
            OUTPUT_PATH.write_text(json.dumps({
                "artist_id": ARTIST_ID, "artist_name": "Poolpat",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "total_playlists": 0, "playlists": []
            }, indent=2))
        sys.exit(0)

    print("=== Poolpat Playlist Tracker ===")

    print("\n[1/4] Authenticating with Spotify...")
    token = get_spotify_token()
    print("  OK")

    print("\n[2/4] Fetching artist tracks...")
    tracks = get_artist_tracks(token)
    print(f"  Found {len(tracks)} tracks")

    print("\n[3/4] Searching for playlists...")
    playlist_ids: set[str] = set()

    for pid in search_playlists_for_artist(token):
        playlist_ids.add(pid)
    print(f"  Artist name search: {len(playlist_ids)} playlists")

    for track in tracks:
        for pid in search_playlists_for_track(track["name"], token):
            playlist_ids.add(pid)
        time.sleep(0.25)
    print(f"  Total unique playlist IDs: {len(playlist_ids)}")

    print("\n[4/4] Enriching via Playlistcheck...")
    enriched = []
    for i, pid in enumerate(sorted(playlist_ids), 1):
        print(f"  [{i}/{len(playlist_ids)}] {pid}")
        data = enrich_playlist(pid)
        if data:
            enriched.append({
                "playlist_id": pid,
                "name": data.get("name") or data.get("playlist_name", ""),
                "followers": data.get("followers") or data.get("follower_count", 0),
                "curator": data.get("curator") or data.get("owner", ""),
                "curator_email": data.get("curator_email") or data.get("email", ""),
                "spotify_url": f"https://open.spotify.com/playlist/{pid}",
                "track_count": data.get("track_count") or data.get("tracks", 0),
                "last_updated": data.get("last_updated", ""),
            })
        time.sleep(0.5)

    enriched.sort(key=lambda x: x.get("followers") or 0, reverse=True)

    output = {
        "artist_id": ARTIST_ID,
        "artist_name": "Poolpat",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_playlists": len(enriched),
        "playlists": enriched,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    total_reach = sum(p.get("followers") or 0 for p in enriched)
    print(f"\n\u2705 {len(enriched)} playlists enriched \u2192 {OUTPUT_PATH}")
    print(f"   Total reach: {total_reach:,} followers")
    if enriched:
        print(f"   Top: {enriched[0]['name']} ({enriched[0].get('followers', 0):,} followers)")


if __name__ == "__main__":
    main()
