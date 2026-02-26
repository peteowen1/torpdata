library(arrow)
library(dplyr)

# Player ratings - rename ppg and convert season totals to per-game rates
season_file <- list.files("source", pattern = "^player_season_ratings_", full.names = TRUE)
if (length(season_file) == 0) stop("No player_season_ratings file found in source/")
if (length(season_file) > 1) {
  season_file <- max(season_file)
  message("Multiple player_season_ratings files found, using: ", season_file)
} else {
  season_file <- season_file[1]
}
season <- read_parquet(season_file)

ratings <- season |>
  filter(games > 0) |>
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

# Team ratings - most recent round of the most recent season
teams <- read_parquet("source/team_ratings.parquet")
latest_teams <- teams |>
  filter(season == max(season)) |>
  filter(round == max(as.numeric(round)))

# Match predictions - rename columns, round values, and format for blog
pred_file <- list.files("source", pattern = "^predictions_", full.names = TRUE)
if (length(pred_file) == 0) stop("No predictions file found in source/")
if (length(pred_file) > 1) {
  pred_file <- max(pred_file)
  message("Multiple predictions files found, using: ", pred_file)
} else {
  pred_file <- pred_file[1]
}
preds <- read_parquet(pred_file) |>
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

stopifnot(nrow(ratings) > 100, nrow(latest_teams) >= 18, nrow(preds) > 0)

dir.create("blog", showWarnings = FALSE)
write_parquet(ratings, "blog/torp_ratings.parquet")
write_parquet(latest_teams, "blog/torp_team_ratings.parquet")
write_parquet(preds, "blog/torp_predictions.parquet")
cat("torp_ratings:", nrow(ratings), "players\n")
cat("torp_team_ratings:", nrow(latest_teams), "teams\n")
cat("torp_predictions:", nrow(preds), "matches\n")
