# poolpat-portfolio

Unified artist portfolio for **Poolpat** — live play stats, full discography, and liner notes across SoundCloud, Spotify, and Apple Music.

**Live site:** [thepoolpat.github.io/poolpat-portfolio](https://thepoolpat.github.io/poolpat-portfolio/)

## Pages

| Route | Description |
|-------|-------------|
| `/` | Home — hero carousel, live stats summary, platform links |
| `/stats/` | Play count trends (Chart.js), per-platform breakdowns, top tracks |
| `/discography/` | Release card grid with artwork, chart badges |
| `/releases/[slug]/` | Per-release liner notes, track listings, Apple Music CTAs |

## Stack

- **Framework:** [Astro](https://astro.build/) 6.x (static output, zero JS by default)
- **Data pipeline:** Python (`pipeline/fetch_plays.py`) — fetches SoundCloud, Spotify, Apple Music daily via GitHub Actions
- **Spotify auth:** OAuth 2.0 Authorization Code Flow with PKCE (user-scoped data)
- **Affiliate links:** `packages/affiliate-helper/js/` — validated Apple Music URLs with campaign tracking
- **Design system:** Dark/light theme (system fonts, Apple Music red accent)
- **Charts:** Chart.js 4 (loaded async via ES module import, non-blocking)

## Project Structure

```
├── .github/workflows/
│   ├── deploy.yml              # Build Astro → deploy to GitHub Pages
│   └── fetch-plays.yml         # Daily Python pipeline (cron 07:15 UTC)
├── packages/affiliate-helper/  # Apple Music affiliate link builder (JS)
├── pipeline/
│   ├── fetch_plays.py          # Multi-platform play count scraper
│   ├── spotify_auth.py         # Spotify PKCE OAuth flow
│   ├── spotify_client.py       # Spotify API client (typed, with retry)
│   ├── spotify_errors.py       # Typed error hierarchy
│   ├── requirements.txt
│   └── tests/                  # 20 unit tests
├── examples/                   # Runnable Spotify demos
├── data/                       # Auto-committed by pipeline
│   ├── plays.json              # Latest play count snapshot
│   ├── history.csv             # Time-series (appended daily)
│   └── rss_tracks.json         # SoundCloud catalog
├── public/artwork/             # Release cover images (1200px, web-optimized)
└── src/
    ├── data/releases.json      # Hand-maintained release catalog (7 releases)
    ├── layouts/Base.astro      # HTML shell, SEO, a11y
    ├── components/             # Astro components
    ├── pages/                  # Route pages
    └── styles/global.css       # Unified design tokens
```

## Setup

```bash
# Install dependencies
npm install
pip3 install -r pipeline/requirements.txt

# Development server
npm run dev

# Production build (public GitHub Pages)
DEPLOY_TARGET=public npm run build

# TypeScript check
npm run check
```

### Spotify Setup (one-time)

1. Register an app at [developer.spotify.com/dashboard](https://developer.spotify.com/dashboard)
2. Add redirect URI: `http://127.0.0.1:8888/callback`
3. Run the auth flow:
   ```bash
   python3 pipeline/spotify_auth.py
   # Enter your Client ID when prompted
   # Authorize in browser → get refresh token
   ```
4. Add secrets to [GitHub Actions](../../settings/secrets/actions):
   - `SPOTIFY_CLIENT_ID`
   - `SPOTIFY_CLIENT_SECRET`
   - `SPOTIFY_REFRESH_TOKEN`

### Running the Pipeline Locally

```bash
# Create .env (not committed)
cp pipeline/.env.example .env
# Fill in your credentials, then:

set -a && source .env && set +a
python3 pipeline/fetch_plays.py
```

> **Note:** Use `set -a` before `source .env` to export the variables. Plain `source .env` sets them but doesn't export to subprocesses.

## Data Pipeline

The Python fetcher runs daily at 07:15 UTC via GitHub Actions:

1. **Spotify (user-scoped):** PKCE refresh token → top tracks (short/medium/long term), recently played
2. **Spotify (public):** Client Credentials → catalog popularity scores (0-100)
3. **SoundCloud:** RSS feed + v2 API → play counts, catalog metadata
4. **Apple Music:** API or web scraping → track catalog (plays are manual entry)
5. Enforces a **monotonic invariant** — play counts never decrease between fetches
6. Commits updated `data/` files to `main` → triggers site rebuild

### Required Secrets

| Secret | Purpose |
|--------|---------|
| `SPOTIFY_CLIENT_ID` | Spotify API access |
| `SPOTIFY_CLIENT_SECRET` | Spotify API access |
| `SPOTIFY_REFRESH_TOKEN` | User-scoped Spotify data (PKCE) |
| `APPLE_MUSIC_TOKEN` | Apple Music API access (optional — requires paid Developer Program) |

### Platform Notes

- **Spotify:** `total_streams` and `monthly_listeners` are preserved from manual entry (Spotify for Artists). The API provides popularity scores and top tracks.
- **Apple Music:** Play counts are manual entry only. API token requires paid Apple Developer Program ($99/yr). Data preserved at current values when token unavailable.
- **SoundCloud:** Fully automated via v2 API + RSS feed.

## Affiliate Attribution

Every Apple Music link is built through the validated `link()` helper (`packages/affiliate-helper/js/`):
- Token: `1000l3chz`
- Campaign format: `web-portfolio-{slug}-{placement}`
- Max 64 characters, validated at build time

## Origin

This repo unifies two earlier projects:

| Project | What it contributed |
|---------|-------------------|
| [poolpat-plays](https://github.com/thepoolpat/poolpat-plays) | Data pipeline, GitHub Actions workflow, play count data |
