library(arrow)
library(dplyr)

# Player ratings - predictive TORP ratings (career-weighted with exponential decay)
all_ratings <- read_parquet("source/torp_ratings.parquet")

ratings <- all_ratings |>
  filter(season == max(season)) |>
  filter(round == max(round)) |>
  select(player_id, player_name, team, position, torp, torp_recv, torp_disp,
         torp_spoil, torp_hitout, gms, season) |>
  arrange(desc(torp))

# Team ratings - most recent round of the most recent season
teams <- read_parquet("source/team_ratings.parquet")
latest_teams <- teams |>
  filter(season == max(season)) |>
  filter(round == max(as.numeric(round)))

# Match predictions - all seasons, handle both processed (2026+) and raw (2025) formats
pred_files <- list.files("source", pattern = "^predictions_", full.names = TRUE)
if (length(pred_files) == 0) stop("No predictions files found in source/")

preds_list <- lapply(pred_files, function(f) {
  season <- as.integer(sub(".*predictions_(\\d+)\\.parquet$", "\\1", basename(f)))
  pred_raw <- read_parquet(f) |> ungroup()

  if ("week" %in% names(pred_raw)) {
    pred_raw |>
      transmute(
        season = !!season,
        round = week,
        home_team = as.character(home_team),
        away_team = as.character(away_team),
        home_rating = round(home_rating, 1),
        away_rating = round(away_rating, 1),
        pred_margin = round(pred_margin, 1),
        home_win_prob = round(pred_win, 3),
        pred_total = round(pred_xtotal, 0),
        actual_margin = margin,
        start_time = if ("start_time" %in% names(pred_raw)) start_time else NA_character_,
        venue = if ("venue" %in% names(pred_raw)) as.character(venue) else NA_character_
      )
  } else {
    pred_raw |>
      filter(team_type == "home") |>
      transmute(
        season = !!season,
        round = round.roundNumber.x,
        home_team = as.character(home_team),
        away_team = as.character(away_team),
        home_rating = round(torp.x, 1),
        away_rating = round(torp.y, 1),
        pred_margin = round(pred_score_diff, 1),
        home_win_prob = round(pred_win, 3),
        pred_total = round(pred_tot_xscore, 0),
        actual_margin = score_diff
      )
  }
})
preds <- bind_rows(preds_list) |> arrange(season, round, desc(abs(pred_margin)))

# Player details - bio data for player profile pages
details_file <- list.files("source", pattern = "^player_details_", full.names = TRUE)
if (length(details_file) == 0) stop("No player_details file found in source/")
details_file <- max(details_file)
details <- read_parquet(details_file) |>
  transmute(
    player_id = providerId,
    player_name,
    team,
    position,
    jumper_number = jumperNumber,
    height_cm = heightInCm,
    weight_kg = weightInKg,
    date_of_birth = dateOfBirth,
    draft_year = draftYear,
    debut_year = debutYear,
    recruited_from = recruitedFrom,
    season
  )

# Game logs - per-game TORP ratings (up to 5 seasons, depending on source data)
game_files <- list.files("source", pattern = "^player_game_ratings_", full.names = TRUE)
if (length(game_files) == 0) stop("No player_game_ratings files found in source/")
game_logs <- lapply(game_files, read_parquet) |>
  bind_rows() |>
  transmute(
    player_id,
    player_name,
    season,
    round,
    team,
    opp,
    torp = total_points,
    torp_recv = recv_points,
    torp_disp = disp_points,
    torp_spoil = spoil_points,
    torp_hitout = hitout_points,
    match_id
  ) |>
  arrange(player_id, season, round)

stopifnot(nrow(ratings) > 100, nrow(latest_teams) >= 18, nrow(preds) > 0)
stopifnot(nrow(details) > 0, nrow(game_logs) > 0)

dir.create("blog", showWarnings = FALSE)
write_parquet(ratings, "blog/torp_ratings.parquet")
write_parquet(latest_teams, "blog/torp_team_ratings.parquet")
write_parquet(preds, "blog/torp_predictions.parquet")
write_parquet(details, "blog/torp_player_details.parquet")
write_parquet(game_logs, "blog/torp_game_logs.parquet")
cat("torp_ratings:", nrow(ratings), "players\n")
cat("torp_team_ratings:", nrow(latest_teams), "teams\n")
cat("torp_predictions:", nrow(preds), "matches\n")
cat("torp_player_details:", nrow(details), "players\n")
cat("torp_game_logs:", nrow(game_logs), "game records\n")
