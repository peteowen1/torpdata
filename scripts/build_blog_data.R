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
