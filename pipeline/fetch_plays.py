#!/usr/bin/env python3
"""Fetch play counts for Poolpat across SoundCloud, Spotify, and Apple Music.

Key invariant: streaming counts are MONOTONICALLY INCREASING.
A new fetch must never reduce any play count below the previously recorded value.
If a fetch returns lower numbers (API failure, missing data), preserve the existing data.
"""

import csv
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = Path(__file__).parent.parent / "data"
PLAYS_JSON = DATA_DIR / "plays.json"
HISTORY_CSV = DATA_DIR / "history.csv"
FAIL_TRACKER = DATA_DIR / ".fetch_failures.json"

SOUNDCLOUD_URL = "https://soundcloud.com/poolpat"
SOUNDCLOUD_USER_ID = 3265651
SOUNDCLOUD_RSS = f"https://feeds.soundcloud.com/users/soundcloud:users:{SOUNDCLOUD_USER_ID}/sounds.rss"
SPOTIFY_ARTIST_ID = "4rr3o9anpUXitNXo0W4uX7"
APPLE_MUSIC_ARTIST_ID = "1716939831"
APPLE_MUSIC_ARTIST_URL = f"https://music.apple.com/us/artist/poolpat/{APPLE_MUSIC_ARTIST_ID}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

ALERT_THRESHOLD = 3  # consecutive failures before creating a GitHub Issue


# ─── Monotonic helpers ───────────────────────────────────────────────────────

def monotonic_merge_tracks(existing: dict, fetched: dict) -> dict:
    """Merge track dicts: keep the MAX of existing vs fetched per track.
    Never allow a track's play count to decrease.
    New tracks from fetched are added. Existing tracks not in fetched are kept."""
    merged = dict(existing)  # start with all existing tracks
    for title, plays in fetched.items():
        if not isinstance(plays, (int, float)) or plays <= 0:
            continue  # skip zero/null fetched values — keep existing
        old = merged.get(title, 0) or 0
        merged[title] = max(old, plays)
    return merged


def monotonic_total(existing_total: int, computed_total: int) -> int:
    """Return the higher of existing vs computed total. Streams never decrease."""
    return max(existing_total or 0, computed_total or 0)


def get_last_history_row() -> dict:
    """Read the last row of history.csv to enforce monotonic totals."""
    if not HISTORY_CSV.exists():
        return {}
    try:
        with open(HISTORY_CSV) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            return rows[-1] if rows else {}
    except Exception:
        return {}


# ─── SoundCloud ──────────────────────────────────────────────────────────────

def fetch_soundcloud_rss() -> list[dict]:
    """Parse the SoundCloud RSS feed — reliable, always returns track catalog (no play counts)."""
    tracks = []
    try:
        resp = requests.get(SOUNDCLOUD_RSS, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
        for item in root.findall(".//item"):
            title = item.findtext("title", "Unknown")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            duration = item.findtext("itunes:duration", "", ns)
            enclosure = item.find("enclosure")
            stream_url = enclosure.get("url", "") if enclosure is not None else ""
            img = item.find("itunes:image", ns)
            artwork = img.get("href", "") if img is not None else ""
            tracks.append({
                "title": title, "link": link, "pub_date": pub_date,
                "duration": duration, "stream_url": stream_url, "artwork": artwork,
            })
        print(f"  RSS: {len(tracks)} tracks from feed")
    except Exception as e:
        print(f"  RSS Error: {e}", file=sys.stderr)
    return tracks


def _get_soundcloud_client_id() -> str | None:
    """Try to extract a client_id from SoundCloud's JS bundles."""
    try:
        resp = requests.get("https://soundcloud.com", headers=HEADERS, timeout=15)
        for match in re.findall(r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', resp.text):
            js_resp = requests.get(match, headers=HEADERS, timeout=10)
            cid_match = re.search(r'client_id:"([a-zA-Z0-9]+)"', js_resp.text)
            if cid_match:
                return cid_match.group(1)
    except Exception:
        pass
    return None


def fetch_soundcloud_plays_v2(client_id: str | None = None) -> dict[str, int]:
    """Fetch play counts via SoundCloud's v2 API with pagination."""
    tracks = {}
    try:
        if not client_id:
            client_id = _get_soundcloud_client_id()

        offset = 0
        limit = 50
        while True:
            url = (
                f"https://api-v2.soundcloud.com/users/{SOUNDCLOUD_USER_ID}/tracks"
                f"?limit={limit}&offset={offset}"
            )
            if client_id:
                url += f"&client_id={client_id}"

            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"  v2 API: HTTP {resp.status_code} at offset {offset}")
                break

            data = resp.json()
            collection = data.get("collection", [])
            if not collection:
                break

            for item in collection:
                title = item.get("title", "Unknown")
                plays = item.get("playback_count", 0) or 0
                tracks[title] = plays

            next_href = data.get("next_href")
            if not next_href or len(collection) < limit:
                break
            offset += limit

        print(f"  v2 API: {len(tracks)} tracks with play counts")
    except Exception as e:
        print(f"  v2 API Error: {e}", file=sys.stderr)
    return tracks


def fetch_soundcloud_profile(client_id: str | None = None) -> dict:
    """Fetch profile info (track_count, followers_count)."""
    try:
        url = f"https://api-v2.soundcloud.com/users/{SOUNDCLOUD_USER_ID}"
        if client_id:
            url += f"?client_id={client_id}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def fetch_soundcloud_all(existing_sc: dict) -> tuple[dict, list[dict], bool]:
    """
    Fetch SoundCloud data. Returns (sc_data_dict, rss_tracks, got_plays).

    CRITICAL: Only update play counts if the API returned real numbers (total > 0).
    RSS feed is always used for the track catalog (rss_tracks.json) but NEVER
    for play counts (it doesn't have them).
    """
    print("\n--- SoundCloud ---")

    client_id = _get_soundcloud_client_id()
    rss_tracks = fetch_soundcloud_rss()
    api_plays = fetch_soundcloud_plays_v2(client_id)
    profile = fetch_soundcloud_profile(client_id)

    # Did the API return any real play counts?
    api_total = sum(api_plays.values())
    got_plays = api_total > 0

    existing_tracks = existing_sc.get("tracks", {})
    existing_total = existing_sc.get("total_plays", 0) or 0

    if got_plays:
        # Merge API data with existing, keeping the MAX per track (monotonic)
        merged = monotonic_merge_tracks(existing_tracks, api_plays)

        # Also add any RSS-only titles not in API or existing (new releases)
        for rt in rss_tracks:
            title = rt["title"]
            if title not in merged:
                # Check fuzzy match
                fuzzy = next((k for k in merged if k.lower().strip() == title.lower().strip()), None)
                if not fuzzy:
                    merged[title] = 0  # new track, no plays yet

        new_total = sum(v for v in merged.values() if isinstance(v, (int, float)))
        final_total = monotonic_total(existing_total, new_total)

        print(f"  Merged: {len(merged)} tracks, {final_total:,} total plays (was {existing_total:,})")

        sc_data = {
            "url": SOUNDCLOUD_URL,
            "tracks": merged,
            "total_plays": final_total,
            "total_tracks": profile.get("track_count", len(merged)),
            "followers": profile.get("followers_count", existing_sc.get("followers", 0)),
            "fetch_status": "success",
            "last_successful_fetch": datetime.now(timezone.utc).isoformat(),
        }
        # Preserve manually-entered Insights metrics (not available via API)
        for key in ("total_plays_all_platforms", "total_listeners",
                    "total_streams_sc", "total_downloads", "source"):
            if key in existing_sc:
                sc_data[key] = existing_sc[key]
    else:
        # API failed — preserve ALL existing data, only update catalog from RSS
        print(f"  WARN: API returned 0 plays, preserving existing data ({existing_total:,} plays)")

        sc_data = dict(existing_sc)
        sc_data["fetch_status"] = "api_failed_preserved"
        # Update profile info if we got it
        if profile:
            sc_data["total_tracks"] = profile.get("track_count", sc_data.get("total_tracks", 0))
            sc_data["followers"] = profile.get("followers_count", sc_data.get("followers", 0))

    return sc_data, rss_tracks, got_plays


# ─── Spotify ─────────────────────────────────────────────────────────────────

def _get_spotify_token() -> str | None:
    """Get Spotify access token via client credentials or anonymous."""
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    if client_id and client_secret:
        try:
            resp = requests.post(
                "https://accounts.spotify.com/api/token",
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret), timeout=30,
            )
            if resp.status_code == 200:
                return resp.json().get("access_token")
        except Exception as e:
            print(f"  Client credentials error: {e}", file=sys.stderr)
    # Fallback: anonymous
    try:
        resp = requests.get(
            "https://open.spotify.com/get_access_token?reason=transport&productType=web_player",
            headers=HEADERS, timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("accessToken")
    except Exception:
        pass
    return None


def fetch_spotify_all(existing_sp: dict) -> tuple[dict, bool]:
    """
    Fetch Spotify data. Returns (sp_data_dict, got_data).

    If SPOTIFY_REFRESH_TOKEN is set, uses the user-scoped Authorization Code
    flow (PKCE) to fetch top tracks and recently played with real data.
    Otherwise falls back to Client Credentials (public popularity scores only).
    """
    print("\n--- Spotify ---")

    # ── Try user-scoped auth first (PKCE refresh token) ──
    refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")

    user_data_fetched = False
    top_tracks_short = []
    top_tracks_medium = []
    top_tracks_long = []
    recently_played = []

    if refresh_token and client_id:
        try:
            from spotify_auth import refresh_access_token
            from spotify_client import SpotifyClient

            tokens = refresh_access_token(client_id, refresh_token)
            client = SpotifyClient(tokens["access_token"], client_id, refresh_token)

            user = client.get_current_user()
            print(f"  Authenticated as: {user.get('display_name', 'unknown')} ({user.get('product', 'free')})")

            top_tracks_short = [
                {"name": t["name"], "popularity": t.get("popularity", 0)}
                for t in client.get_top_tracks("short_term", 50)
            ]
            top_tracks_medium = [
                {"name": t["name"], "popularity": t.get("popularity", 0)}
                for t in client.get_top_tracks("medium_term", 50)
            ]
            top_tracks_long = [
                {"name": t["name"], "popularity": t.get("popularity", 0)}
                for t in client.get_top_tracks("long_term", 50)
            ]
            recently_played = [
                {"name": t["track"]["name"], "played_at": t["played_at"]}
                for t in client.get_recently_played(50)
            ]

            print(f"  User data: {len(top_tracks_short)} short-term, {len(top_tracks_medium)} medium-term, {len(top_tracks_long)} long-term top tracks")
            print(f"  Recently played: {len(recently_played)} tracks")
            user_data_fetched = True

        except Exception as e:
            print(f"  User auth failed ({e}), falling back to public API", file=sys.stderr)

    # ── Public API: catalog + popularity scores ──
    token = _get_spotify_token()
    if not token and not user_data_fetched:
        print("  ERROR: No Spotify token", file=sys.stderr)
        sp_data = dict(existing_sp) if existing_sp else {}
        sp_data["fetch_status"] = "failed"
        return sp_data, False

    fetched_tracks = {}
    if token:
        api_headers = {"Authorization": f"Bearer {token}"}

        try:
            album_ids = set()
            for group in ["album", "single", "appears_on", "compilation"]:
                offset = 0
                while True:
                    resp = requests.get(
                        f"https://api.spotify.com/v1/artists/{SPOTIFY_ARTIST_ID}/albums",
                        headers=api_headers,
                        params={"include_groups": group, "market": "US", "limit": 50, "offset": offset},
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        break
                    data = resp.json()
                    for album in data.get("items", []):
                        album_ids.add(album["id"])
                    if not data.get("next"):
                        break
                    offset += 50

            print(f"  Found {len(album_ids)} albums/singles")

            track_ids = set()
            album_list = list(album_ids)
            for i in range(0, len(album_list), 20):
                batch = album_list[i:i+20]
                resp = requests.get(
                    "https://api.spotify.com/v1/albums",
                    headers=api_headers,
                    params={"ids": ",".join(batch), "market": "US"}, timeout=30,
                )
                if resp.status_code == 200:
                    for album in resp.json().get("albums", []):
                        if not album:
                            continue
                        for track in album.get("tracks", {}).get("items", []):
                            if SPOTIFY_ARTIST_ID in [a["id"] for a in track.get("artists", [])]:
                                track_ids.add(track["id"])

            print(f"  Found {len(track_ids)} tracks by Poolpat")

            for i in range(0, len(list(track_ids)), 50):
                batch = list(track_ids)[i:i+50]
                resp = requests.get(
                    "https://api.spotify.com/v1/tracks",
                    headers=api_headers,
                    params={"ids": ",".join(batch), "market": "US"}, timeout=30,
                )
                if resp.status_code == 200:
                    for track in resp.json().get("tracks", []):
                        if track:
                            fetched_tracks[track["name"]] = track.get("popularity", 0)

        except Exception as e:
            print(f"  Spotify catalog error: {e}", file=sys.stderr)

    got_data = len(fetched_tracks) > 0 or user_data_fetched

    existing_total_streams = existing_sp.get("total_streams", 0) or 0
    existing_monthly = existing_sp.get("monthly_listeners")
    existing_streams_28d = existing_sp.get("streams_28d")
    existing_source = existing_sp.get("source", "")

    if got_data:
        existing_tracks = existing_sp.get("tracks", {})
        merged = monotonic_merge_tracks(existing_tracks, fetched_tracks) if fetched_tracks else dict(existing_tracks)

        sp_data = {
            "url": f"https://open.spotify.com/artist/{SPOTIFY_ARTIST_ID}",
            "artist_id": SPOTIFY_ARTIST_ID,
            "source": existing_source if existing_source else "Spotify API (popularity 0-100)",
            "tracks": merged,
            "total_streams": existing_total_streams,
            "total_tracks": max(int(existing_sp.get("total_tracks", 0) or 0), len(merged), 21),
            "monthly_listeners": existing_monthly,
            "streams_28d": existing_streams_28d,
            "fetch_status": "success",
            "last_successful_fetch": datetime.now(timezone.utc).isoformat(),
            "auth_method": "user_token" if user_data_fetched else "client_credentials",
            "note": "total_streams/monthly_listeners from Spotify for Artists (manual). Track values are popularity scores.",
        }

        if user_data_fetched:
            sp_data["top_tracks_short"] = top_tracks_short
            sp_data["top_tracks_medium"] = top_tracks_medium
            sp_data["top_tracks_long"] = top_tracks_long
            sp_data["recently_played"] = recently_played

    else:
        sp_data = dict(existing_sp) if existing_sp else {}
        sp_data["fetch_status"] = "failed"

    print(f"  Result: {len(fetched_tracks)} catalog tracks, total_streams={existing_total_streams:,} (preserved), user_data={'yes' if user_data_fetched else 'no'}")
    return sp_data, got_data


# ─── Apple Music ─────────────────────────────────────────────────────────────

def fetch_apple_music_all(existing_am: dict) -> tuple[dict, bool]:
    """
    Apple Music: public API has no play counts.
    All play data comes from manual entry (Apple Music for Artists).
    This function only updates catalog metadata, never touches play counts.
    """
    print("\n--- Apple Music ---")
    track_names = []
    try:
        apple_token = os.environ.get("APPLE_MUSIC_TOKEN")
        if apple_token:
            resp = requests.get(
                f"https://api.music.apple.com/v1/catalog/us/artists/{APPLE_MUSIC_ARTIST_ID}/songs",
                headers={"Authorization": f"Bearer {apple_token}", "User-Agent": HEADERS["User-Agent"]},
                timeout=30,
            )
            if resp.status_code == 200:
                for song in resp.json().get("data", []):
                    track_names.append(song.get("attributes", {}).get("name", "Unknown"))
                print(f"  Apple Music API: {len(track_names)} catalog tracks")
        if not track_names:
            resp = requests.get(APPLE_MUSIC_ARTIST_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "MusicGroup":
                        for track in data.get("track", []):
                            track_names.append(track.get("name", "Unknown"))
                except (json.JSONDecodeError, TypeError):
                    continue
            print(f"  Apple Music scrape: {len(track_names)} catalog tracks")
    except Exception as e:
        print(f"  Apple Music error: {e}", file=sys.stderr)

    # Always preserve existing manual data
    has_manual = bool(existing_am.get("tracks")) and any(
        v is not None and v > 0 for v in existing_am.get("tracks", {}).values()
    )

    if has_manual:
        am_data = dict(existing_am)
        am_data["fetch_status"] = "preserved"
        if track_names:
            am_data["catalog_track_count"] = len(track_names)
        return am_data, True
    else:
        am_data = {
            "url": APPLE_MUSIC_ARTIST_URL,
            "tracks": {name: None for name in track_names},
            "total_tracks": max(int(existing_am.get("total_tracks", 0) or 0), len(track_names), 21),
            "note": "Apple Music does not expose play counts. Update manually from Apple Music for Artists.",
            "fetch_status": "catalog_only" if track_names else "failed",
        }
        return am_data, len(track_names) > 0


# ─── Failure tracking & alerting ─────────────────────────────────────────────

def load_failure_tracker() -> dict:
    if FAIL_TRACKER.exists():
        with open(FAIL_TRACKER) as f:
            return json.load(f)
    return {"soundcloud": 0, "spotify": 0, "apple_music": 0}


def save_failure_tracker(tracker: dict) -> None:
    with open(FAIL_TRACKER, "w") as f:
        json.dump(tracker, f)


def create_alert_issue(platform: str, consecutive: int) -> None:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        print(f"  ALERT: {platform} failed {consecutive}x (no GH token)")
        return
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/issues",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={
                "title": f"🔴 {platform} fetch failed {consecutive}x consecutively",
                "body": (
                    f"The automated fetcher failed to retrieve data from **{platform}** "
                    f"for **{consecutive} consecutive runs**.\n\n"
                    f"Check the workflow logs and update the fetcher.\n\n"
                    f"_Auto-created by `fetch_plays.py`._"
                ),
                "labels": ["bug", "automated"],
            },
            timeout=30,
        )
        if resp.status_code == 201:
            print(f"  ALERT: Created GitHub Issue for {platform}")
    except Exception as e:
        print(f"  ALERT error: {e}", file=sys.stderr)


# ─── Data I/O ────────────────────────────────────────────────────────────────

def load_existing_data() -> dict:
    if PLAYS_JSON.exists():
        with open(PLAYS_JSON) as f:
            return json.load(f)
    return {}


def save_plays_json(data: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PLAYS_JSON, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {PLAYS_JSON}")


def save_rss_tracks(rss_tracks: list[dict]) -> None:
    rss_data = {
        "rss_feed_url": SOUNDCLOUD_RSS,
        "soundcloud_user_id": SOUNDCLOUD_USER_ID,
        "artist": "Poolpat",
        "link": "https://ffm.bio/poolpat",
        "last_build_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_tracks_in_feed": len(rss_tracks),
        "tracks": [
            {
                "title": t["title"], "date": t.get("pub_date", ""),
                "url": t.get("link", ""), "duration": t.get("duration", ""),
                "stream_url": t.get("stream_url", ""), "artwork": t.get("artwork", ""),
            }
            for t in rss_tracks
        ],
    }
    with open(DATA_DIR / "rss_tracks.json", "w") as f:
        json.dump(rss_data, f, indent=2, ensure_ascii=False)


def append_history_csv(data: dict) -> None:
    """Append a history row. MONOTONIC: never record a value lower than the previous row."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = HISTORY_CSV.exists()

    last = get_last_history_row()
    prev_sc = int(last.get("soundcloud_total_plays", 0) or 0)
    prev_sp = int(last.get("spotify_total_streams", last.get("spotify_total_popularity", 0)) or 0)
    prev_am = int(last.get("apple_music_total_plays", 0) or 0)

    sc = data.get("soundcloud", {})
    sp = data.get("spotify", {})
    am = data.get("apple_music", {})

    sc_total = max(prev_sc, sc.get("total_plays", 0) or 0)
    sp_total = max(prev_sp, sp.get("total_streams", 0) or 0)
    am_total = max(prev_am, am.get("total_plays", 0) or 0)

    fieldnames = [
        "timestamp", "soundcloud_total_plays", "soundcloud_track_count",
        "spotify_total_streams", "spotify_track_count",
        "apple_music_total_plays", "apple_music_track_count",
    ]

    with open(HISTORY_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": data.get("last_updated", datetime.now(timezone.utc).isoformat()),
            "soundcloud_total_plays": sc_total,
            "soundcloud_track_count": len(sc.get("tracks", {})),
            "spotify_total_streams": sp_total,
            "spotify_track_count": len(sp.get("tracks", {})),
            "apple_music_total_plays": am_total,
            "apple_music_track_count": len(am.get("tracks", {})),
        })
    print(f"History: SC={sc_total:,} SP={sp_total:,} AM={am_total:,}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc).isoformat()
    print(f"Fetching play counts at {now}")

    existing = load_existing_data()
    failures = load_failure_tracker()

    # ── SoundCloud ──
    sc_data, rss_tracks, sc_ok = fetch_soundcloud_all(existing.get("soundcloud", {}))
    if sc_ok:
        failures["soundcloud"] = 0
    else:
        failures["soundcloud"] += 1
        if failures["soundcloud"] >= ALERT_THRESHOLD:
            create_alert_issue("SoundCloud", failures["soundcloud"])

    if rss_tracks:
        save_rss_tracks(rss_tracks)

    # ── Spotify ──
    sp_data, sp_ok = fetch_spotify_all(existing.get("spotify", {}))
    if sp_ok:
        failures["spotify"] = 0
    else:
        failures["spotify"] += 1
        if failures["spotify"] >= ALERT_THRESHOLD:
            create_alert_issue("Spotify", failures["spotify"])

    # ── Apple Music ──
    am_data, am_ok = fetch_apple_music_all(existing.get("apple_music", {}))
    if am_ok:
        failures["apple_music"] = 0
    else:
        failures["apple_music"] += 1
        if failures["apple_music"] >= ALERT_THRESHOLD:
            create_alert_issue("Apple Music", failures["apple_music"])

    save_failure_tracker(failures)

    # ── Assemble & save ──
    data = {
        "artist": "Poolpat",
        "last_updated": now,
        "soundcloud": sc_data,
        "spotify": sp_data,
        "apple_music": am_data,
    }

    save_plays_json(data)
    append_history_csv(data)

    sc_t = sc_data.get("total_plays", 0) or 0
    sp_t = sp_data.get("total_streams", 0) or 0
    am_t = am_data.get("total_plays", 0) or 0
    print(f"\nGrand total: {sc_t + sp_t + am_t:,}")
    print(f"  SC: {sc_t:,} ({len(sc_data.get('tracks', {}))} tracks) [{sc_data.get('fetch_status')}]")
    print(f"  SP: {sp_t:,} ({len(sp_data.get('tracks', {}))} tracks) [{sp_data.get('fetch_status')}]")
    print(f"  AM: {am_t:,} ({len(am_data.get('tracks', {}))} tracks) [{am_data.get('fetch_status')}]")


if __name__ == "__main__":
    main()
