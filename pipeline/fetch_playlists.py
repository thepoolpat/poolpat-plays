"""
fetch_playlists.py

Discovers which Spotify playlists feature Poolpat tracks using:
  1. Spotify Web API — get artist tracks & search featured playlists
  2. Playlistcheck RapidAPI — enrich each playlist with follower count,
     curator contact, and historical data

Outputs: data/playlists.json

Requires env vars:
  SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN
  RAPIDAPI_KEY
"""

import json
import os
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
PLAYLISTCHECK_BASE = "https://playlistcheck.p.rapidapi.com"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"


# ---------------------------------------------------------------------------
# Spotify auth (refresh token flow)
# ---------------------------------------------------------------------------
def get_spotify_token() -> str:
    client_id = os.environ["SPOTIFY_CLIENT_ID"]
    client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]
    refresh_token = os.environ["SPOTIFY_REFRESH_TOKEN"]

    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        auth=(client_id, client_secret),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


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
    """Returns list of {id, name, uri} for all artist tracks via albums."""
    tracks = []
    # Get all albums
    albums_resp = spotify_get(
        f"/artists/{ARTIST_ID}/albums",
        token,
        {"include_groups": "album,single,appears_on", "limit": 50, "market": "IE"},
    )
    album_ids = [a["id"] for a in albums_resp.get("items", [])]

    for album_id in album_ids:
        album_tracks = spotify_get(f"/albums/{album_id}/tracks", token, {"limit": 50})
        for t in album_tracks.get("items", []):
            # Only include tracks where Poolpat is an artist
            artist_ids = [a["id"] for a in t.get("artists", [])]
            if ARTIST_ID in artist_ids:
                tracks.append({"id": t["id"], "name": t["name"], "uri": t["uri"]})

    # Deduplicate by track ID
    seen = set()
    unique = []
    for t in tracks:
        if t["id"] not in seen:
            seen.add(t["id"])
            unique.append(t)
    return unique


def search_track_playlists(track_name: str, token: str) -> list[str]:
    """Search Spotify for playlists containing a track name. Returns playlist IDs."""
    results = spotify_get(
        "/search",
        token,
        {"q": track_name, "type": "playlist", "limit": 20, "market": "IE"},
    )
    items = results.get("playlists", {}).get("items", []) or []
    return [p["id"] for p in items if p]


# ---------------------------------------------------------------------------
# Playlistcheck RapidAPI helpers
# ---------------------------------------------------------------------------
def rapidapi_headers() -> dict:
    return {
        "x-rapidapi-host": "playlistcheck.p.rapidapi.com",
        "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
    }


def playlistcheck_get(endpoint: str, params: dict = None) -> dict | None:
    """Call Playlistcheck API. Returns None on non-fatal errors."""
    try:
        resp = requests.get(
            f"{PLAYLISTCHECK_BASE}{endpoint}",
            headers=rapidapi_headers(),
            params=params or {},
            timeout=20,
        )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            print(f"  Playlistcheck rate limit — sleeping {retry_after}s")
            time.sleep(retry_after)
            return playlistcheck_get(endpoint, params)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Playlistcheck error for {endpoint}: {e}")
        return None


def enrich_playlist(playlist_id: str) -> dict | None:
    """Fetch enriched playlist data from Playlistcheck."""
    data = playlistcheck_get("/playlist", {"playlist_id": playlist_id})
    return data


def search_artist_playlists(artist_name: str) -> list[dict]:
    """Search Playlistcheck for playlists featuring an artist."""
    data = playlistcheck_get("/artist", {"query": artist_name})
    if not data:
        return []
    return data.get("playlists", []) or []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    rapidapi_key = os.environ.get("RAPIDAPI_KEY", "")
    if not rapidapi_key:
        print("ERROR: RAPIDAPI_KEY not set — skipping playlist fetch")
        sys.exit(0)  # Non-fatal: don't break main fetch-plays pipeline

    print("=== Poolpat Playlist Tracker ===")
    print(f"Artist ID: {ARTIST_ID}")

    # --- 1. Get Spotify token ---
    print("\n[1/4] Authenticating with Spotify...")
    token = get_spotify_token()
    print("  OK")

    # --- 2. Get all artist tracks ---
    print("\n[2/4] Fetching artist tracks from Spotify...")
    tracks = get_artist_tracks(token)
    print(f"  Found {len(tracks)} tracks")

    # --- 3. Discover playlists via Spotify search ---
    print("\n[3/4] Discovering playlists via Spotify search...")
    playlist_ids: set[str] = set()
    for track in tracks:
        ids = search_track_playlists(track["name"], token)
        playlist_ids.update(ids)
        time.sleep(0.3)  # Gentle rate limiting
    print(f"  Found {len(playlist_ids)} candidate playlist IDs")

    # Also search by artist name via Playlistcheck directly
    print("  Searching Playlistcheck for artist name...")
    pc_results = search_artist_playlists("Poolpat")
    for p in pc_results:
        pid = p.get("playlist_id") or p.get("id", "")
        if pid:
            playlist_ids.add(pid)
    print(f"  Total unique playlist IDs: {len(playlist_ids)}")

    # --- 4. Enrich each playlist via Playlistcheck ---
    print("\n[4/4] Enriching playlists via Playlistcheck...")
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
                "raw": data,
            })
        time.sleep(0.5)  # Stay within RapidAPI rate limits

    # Sort by followers descending
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
    print(f"\n✅ Saved {len(enriched)} playlists → {OUTPUT_PATH}")

    # Summary
    total_reach = sum(p.get("followers") or 0 for p in enriched)
    print(f"   Total estimated reach: {total_reach:,} playlist followers")
    if enriched:
        print(f"   Biggest playlist: {enriched[0]['name']} ({enriched[0]['followers']:,} followers)")


if __name__ == "__main__":
    main()
