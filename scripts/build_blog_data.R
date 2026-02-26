library(arrow)
library(dplyr)

dir.create("blog", showWarnings = FALSE)

# Player ratings - per-game averages
season_file <- list.files("source", pattern = "^player_season_ratings_", full.names = TRUE)[1]
season <- read_parquet(season_file)

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
write_parquet(ratings, "blog/torp_ratings.parquet")
cat("torp_ratings:", nrow(ratings), "players\n")

# Team ratings - latest season + round
teams <- read_parquet("source/team_ratings.parquet")
latest_teams <- teams |>
  filter(season == max(season)) |>
  filter(round == max(round))
write_parquet(latest_teams, "blog/torp_team_ratings.parquet")
cat("torp_team_ratings:", nrow(latest_teams), "teams\n")

# Match predictions - clean for blog display
pred_file <- list.files("source", pattern = "^predictions_", full.names = TRUE)[1]
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
write_parquet(preds, "blog/torp_predictions.parquet")
cat("torp_predictions:", nrow(preds), "matches\n")
