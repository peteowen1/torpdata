# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

**torpdata** is a data-only repository for AFL analytics. Data is distributed via GitHub releases using piggyback - no data files are stored in git itself. This repo contains only documentation files (README.md, CLAUDE.md).

**No build/test commands exist** - all code lives in the `torp` package which releases data here.

## Data Distribution

Data files are stored as GitHub releases and downloaded on-demand by the `torp` package.

### Release Tags

| Tag | Contents |
|-----|----------|
| `pbp-data` | Play-by-play event data |
| `chains-data` | Possession chain data |
| `player_stats-data` | Player game statistics |
| `xg-data` | Expected goals data |
| `fixtures-data` | Match fixtures and schedules |
| `results-data` | Match results |
| `teams-data` | Team lineup data |
| `player_details-data` | Player biographical data |
| `predictions` | Match predictions |
| `reference-data` | Reference data (plyr_gm_df, torp_df_total) |

### File Naming Convention

Files follow the pattern: `{prefix}_{season}_{round}.parquet`

Examples:
- `pbp_data_2024_01.parquet` - Play-by-play for 2024 round 1
- `fixtures_2024.parquet` - Fixtures for 2024 season
- `chains_data_2024_all.parquet` - All chains for 2024 (aggregated)

## Data Release Workflow

Data is released from the `torp` package using `data-raw/01-data/release_data.R`:

```r
# In torp package
source("data-raw/01-data/release_data.R")

# Or for specific data:
torp::save_to_release(df, "fixtures_2026", "fixtures-data")
```

## Consuming Data

Data is loaded in the `torp` package via `load_*()` functions:

```r
library(torp)

# Load play-by-play data
pbp <- load_pbp(2024, rounds = 1:10)

# Load all chains
chains <- load_chains(TRUE, TRUE)

# Load fixtures
fixtures <- load_fixtures(2024)
```

## Local Development

This repository doesn't contain the actual data files. To work with data locally:

1. Use the `torp` package to download data via `load_*()` functions
2. Data is cached locally by torp's disk cache system
3. Release new data using `torp::save_to_release()`

## Related Packages

- [torp](https://github.com/peteowen1/torp) - AFL analytics package (loads data from here)
- [torpmodels](https://github.com/peteowen1/torpmodels) - Pre-trained models
