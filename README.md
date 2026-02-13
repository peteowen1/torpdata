# torpdata

Processed AFL data for the [torp](https://github.com/peteowen1/torp) analytics package. Data is stored as parquet files on GitHub releases and downloaded on demand.

## Available Data

| Data Type | Function | Coverage | Approx Rows/Season |
|-----------|----------|----------|-------------------|
| Play-by-play | `load_pbp()` | 2021+ | ~320K |
| Chains | `load_chains()` | 2021+ | ~160K |
| Player Stats | `load_player_stats()` | 2021+ | ~10K |
| Expected Goals | `load_xg()` | 2021+ | ~200 |
| Fixtures | `load_fixtures()` | 2021+ | ~200 |
| Results | `load_results()` | 2021+ | ~200 |
| Teams | `load_teams()` | 2021+ | ~10K |
| Player Details | `load_player_details()` | 2021+ | ~800 |
| TORP Ratings | `load_torp_ratings()` | 2021+ | ~113K total |
| Predictions | `load_predictions()` | Current season | Varies |

## Usage

All data is accessed through the `torp` R package:

```r
library(torp)

# Single season
pbp <- load_pbp(2025)

# Specific rounds
pbp <- load_pbp(2025, rounds = 1:10)

# All available data
pbp_all <- load_pbp(TRUE, rounds = TRUE)

# Other data types
stats <- load_player_stats(2025)
fixtures <- load_fixtures(all = TRUE)
results <- load_results(2025)
chains <- load_chains(2025)
```

Data is cached to disk after first download so subsequent calls are instant.

## Data Updates

Data is updated automatically via a daily GitHub Action in the torp repository (2:00 AM AEST). New data is processed and released after each AFL round.

## File Format

All files are Apache Parquet format, named `{prefix}_{season}_{round}.parquet` (e.g., `pbp_data_2025_01.parquet`).

## Related Packages

- [torp](https://github.com/peteowen1/torp) -- Core AFL analytics package
- [torpmodels](https://github.com/peteowen1/torpmodels) -- Pre-trained prediction models
