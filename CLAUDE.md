# CLAUDE.md

## Overview

**torpdata** is a data-only repository — no R package code. It stores processed AFL data as parquet files via GitHub releases (managed by `piggyback`).

See [ARCHITECTURE.md](ARCHITECTURE.md) for full documentation, Mermaid diagrams, and workflow details.

## Structure

```
torpdata/
├── ARCHITECTURE.md  # Full documentation, Mermaid diagrams, workflow details
├── blog/            # Aggregated parquet files for blog/R2 upload (ratings, predictions, simulations, etc.)
├── data/            # Local parquet files (gitignored, populated by torp::download_torp_data())
├── scripts/         # Utility scripts (build_blog_data.R)
├── source/          # Year-by-year intermediates (afl_teams_YYYY.parquet, match_events_YYYY.parquet) consumed by scripts/build_blog_data.R
├── LICENSE
└── README.md
```

## Release Tags

Each data type has its own GitHub release tag:
- `pbp-data`, `chains-data`, `ep_wp_chart-data`
- `fixtures-data`, `player_stats-data`, `player_details-data`
- `player_game-data`, `results-data`, `xg-data`, `teams-data`
- `predictions`, `ratings-data`, `team_ratings-data`
- `player_game_ratings-data`, `player_season_ratings-data`
- `weather-data`, `injury-data`, `retrodictions`, `reference-data`
- `player_stat_ratings-data`, `player_skills-data`, `psr-data`

(The 58 per-stat GAMs and core EP/WP/shot/match models are released via
torpmodels' `stat-models` and `core-models` tags, not stored here.)

## How Data Gets Here

1. **Collection**: `torp/data-raw/01-data/` scripts scrape and process AFL data
2. **Upload**: `save_to_release()` in torp pushes parquet files to GitHub releases on this repo
3. **Download**: `torp::load_*()` functions fetch from these releases; `torp::download_torp_data()` bulk-downloads for offline use

## GitHub Actions

### `daily-data-release.yml`
Runs daily at 16:00 UTC (2:00 AM AEST). Can also be manually dispatched with `force_release` and `rebuild_aggregates` flags.

**Flow:**
1. Checks out torpdata + torp, installs R dependencies
2. Runs `run_daily_release()` (sourced from torp's `data-raw/01-data/daily_release.R`, not an exported function) — returns `'full'`, `'team_only'`, or `'none'`
3. R writes `release_done=true|false` to `$GITHUB_OUTPUT` (derived from the `'none'` vs not check; written via `cat(file=Sys.getenv('GITHUB_OUTPUT'))` to avoid stdout contamination from `devtools::load_all()`)
4. Verification, release info, and summary steps are gated on `release_done == 'true'`
5. If data was released and verified, dispatches `ratings-trigger` to torp
6. Off-season: skips gracefully with a "No New Data" summary

**Manual dispatch:**
```bash
gh workflow run daily-data-release.yml --ref main -f force_release=false -f rebuild_aggregates=false
```

### `lineup-predictions.yml`
Runs on Wed/Thu evenings and Friday morning AEST (when AFL lineups are typically released). Can also be manually dispatched.

**Flow:**
1. Calls `has_new_team_data()` to detect new lineup data
2. If new lineups found, runs `run_daily_release()` for team-only release
3. Dispatches `ratings-trigger` to torp for updated predictions with lineup data

```bash
gh workflow run lineup-predictions.yml --ref main
```

### `build-blog-data.yml`
Blog/analysis data generation workflow. Manually dispatched — runs `scripts/build_blog_data.R` to build derived datasets, then uploads them to Cloudflare R2.

## Cloudflare R2 Integration

Blog data is uploaded to the `inthegame-data` R2 bucket under the `afl/` prefix via wrangler CLI in `build-blog-data.yml`.

**Files uploaded to `afl/`:** `ratings.parquet`, `team-ratings.parquet`, `predictions.parquet`, `player-details.parquet`, `game-logs.parquet`, `game-stats.parquet`, `shots.parquet`, `simulations.parquet`, `player-skills.parquet`, `player-finishing.parquet`, `afl_teams_YYYY.parquet`, `match_events_YYYY.parquet` (per season)

**Name rename at build:** `build_blog_data.R` reads the `torp_`-prefixed release assets (e.g. `source/torp_ratings.parquet` from the `ratings-data` release) and writes the unprefixed blog/R2 names (`ratings.parquet`). So a `torp_*.parquet` on the release/disk side maps to `*.parquet` (no prefix) in `blog/` and R2 — don't expect the names to match across the boundary.

**Secrets required:** `CLOUDFLARE_R2_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` (configured in repo settings)

## Useful Commands

```bash
# List assets in a release tag
gh release view pbp-data --repo peteowen1/torpdata --json assets --jq '.assets[].name'

# Check latest release dates (NB: release.createdAt is when the TAG was
# created, not when assets were uploaded — asset-level updatedAt is truth)
gh release list --repo peteowen1/torpdata --limit 10

# Check actual asset freshness (use this, not release createdAt)
gh -R peteowen1/torpdata release view pbp-data --json assets \
  --jq '[.assets[] | {name: .name, updated: .updatedAt}]'

# Manually trigger daily release
gh workflow run daily-data-release.yml --ref main

# Manually trigger blog data build
gh workflow run build-blog-data.yml --ref main
```

## Local `data/` Directory

The `data/` folder is gitignored and used for local caching. It's auto-detected by `torp::get_local_data_dir()` when working in the torpverse workspace.
