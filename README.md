# torpdata

AFL data repository for the torp package.

## Overview

This repository serves as a data distribution point for AFL analytics data. Data files are stored as GitHub releases and downloaded on-demand by the [torp](https://github.com/peteowen1/torp) package.

## Available Data

| Data Type | Description |
|-----------|-------------|
| Play-by-play | Event-level match data |
| Chains | Possession chain sequences |
| Player Stats | Per-game player statistics |
| Expected Goals | xG model outputs |
| Fixtures | Match schedules |
| Results | Final match scores |
| Teams | Team lineup data |
| Player Details | Player biographical info |

## Usage

Data is accessed through the `torp` R package:
```r
library(torp)

# Load play-by-play data
pbp <- load_pbp(2024)

# Load chains data
chains <- load_chains(2024, rounds = 1:10)

# Load fixtures
fixtures <- load_fixtures(all = TRUE)
```

## Data Updates

Data is updated automatically via GitHub Actions in the torp repository. New data is released after each AFL round.

## Related

- [torp](https://github.com/peteowen1/torp) - AFL analytics package
- [torpmodels](https://github.com/peteowen1/torpmodels) - Pre-trained models
