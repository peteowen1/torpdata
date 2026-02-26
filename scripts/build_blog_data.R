library(arrow)
library(dplyr)

# Player ratings - per-game averages
season_file <- list.files("source", pattern = "^player_season_ratings_", full.names = TRUE)
if (length(season_file) == 0) stop("No player_season_ratings file found in source/")
season <- read_parquet(season_file[1])

ratings <- season |>
  mutate(
    torp = ppg,
    torp_recv = season_recv / games,
    torp_disp = season_disp / games,
    torp_spoil = season_spoil / games,
    torp_hitout = season_hitout / games,
    gms = games
  ) |>
  select(player_name, team, position, torp, torp_recv, torp_disp,
         torp_spoil, torp_hitout, gms, season) |>
  arrange(desc(torp))

# Team ratings - latest season + round
teams <- read_parquet("source/team_ratings.parquet")
latest_teams <- teams |>
  filter(season == max(season)) |>
  filter(round == max(round))

# Match predictions - clean for blog display
pred_file <- list.files("source", pattern = "^predictions_", full.names = TRUE)
if (length(pred_file) == 0) stop("No predictions file found in source/")
preds <- read_parquet(pred_file[1]) |>
  ungroup() |>
  transmute(
    round = week,
    home_team = as.character(home_team),
    away_team = as.character(away_team),
    home_rating = round(home_rating, 1),
    away_rating = round(away_rating, 1),
    pred_margin = round(pred_margin, 1),
    home_win_prob = round(pred_win, 3),
    pred_total = round(pred_xtotal, 0),
    actual_margin = margin
  ) |>
  arrange(round, desc(abs(pred_margin)))

# Validate before writing
stopifnot(nrow(ratings) > 0, nrow(latest_teams) > 0, nrow(preds) > 0)

# Write all outputs
dir.create("blog", showWarnings = FALSE)
write_parquet(ratings, "blog/torp_ratings.parquet")
write_parquet(latest_teams, "blog/torp_team_ratings.parquet")
write_parquet(preds, "blog/torp_predictions.parquet")
cat("torp_ratings:", nrow(ratings), "players\n")
cat("torp_team_ratings:", nrow(latest_teams), "teams\n")
cat("torp_predictions:", nrow(preds), "matches\n")
