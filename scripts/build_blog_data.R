library(arrow)
library(dplyr)

# Player ratings - predictive TORP ratings (career-weighted with exponential decay)
all_ratings <- read_parquet("source/torp_ratings.parquet")
message("Ratings columns: ", paste(names(all_ratings), collapse = ", "))
message("Ratings total rows: ", nrow(all_ratings))
message("Seasons: ", paste(sort(unique(all_ratings$season)), collapse = ", "))
max_season <- max(all_ratings$season)
season_df <- all_ratings[all_ratings$season == max_season, ]
message("Max season ", max_season, " rows: ", nrow(season_df))
message("Rounds in max season: ", paste(sort(unique(season_df$round)), collapse = ", "))
message("Round class: ", class(season_df$round))
message("Round values (table): ", paste(names(table(season_df$round)), table(season_df$round), sep = "=", collapse = ", "))
message("Max round: ", max(season_df$round))

ratings <- all_ratings |>
  filter(season == max(season)) |>
  filter(round == max(round)) |>
  select(player_id, player_name, team, position, torp, torp_recv, torp_disp,
         torp_spoil, torp_hitout, gms, season) |>
  arrange(desc(torp))
message("Filtered ratings rows: ", nrow(ratings))

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
details_raw <- read_parquet(details_file)
# Standardise column names — strip 'player.' prefix from flattened AFL API response.
# Defensive shim for parquets uploaded before get_afl_player_details() was fixed;
# new parquets already have clean names so this is a no-op for them.
names(details_raw) <- sub("^player\\.", "", names(details_raw))
if ("team.name" %in% names(details_raw) && !"team" %in% names(details_raw)) {
  names(details_raw)[names(details_raw) == "team.name"] <- "team"
}
required_detail_cols <- c("providerId", "player_name", "team", "position",
                          "jumperNumber", "heightInCm", "weightInKg",
                          "dateOfBirth", "draftYear", "debutYear",
                          "recruitedFrom")
missing_detail_cols <- setdiff(required_detail_cols, names(details_raw))
if (length(missing_detail_cols) > 0) {
  stop("player_details parquet missing columns after name standardisation: ",
       paste(missing_detail_cols, collapse = ", "),
       "\nActual columns: ", paste(names(details_raw), collapse = ", "))
}
# season may not be in parquet — derive from filename if absent
if (!"season" %in% names(details_raw)) {
  details_raw$season <- as.integer(sub(".*player_details_(\\d{4})\\.parquet$", "\\1", details_file))
}
details <- details_raw |>
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

# Shot data from PBP — optional, skipped if PBP files absent
# Standard AFL field dimensions (metres) — canonical values in torp/R/constants.R
DEFAULT_VENUE_LENGTH <- 165L
DEFAULT_VENUE_WIDTH  <- 135L

pbp_files <- list.files("source", pattern = "^pbp_data_\\d{4}_all\\.parquet$", full.names = TRUE)
shots <- if (length(pbp_files) == 0) {
  message("INFO: No PBP files in source/ — skipping torp_shots.parquet")
  NULL
} else {
  shot_cols <- c("player_id", "season", "round_number", "x", "y", "distance",
                 "goal_prob", "points_shot", "phase_of_play", "venue_length",
                 "venue_width", "shot_at_goal")

  pbp <- lapply(pbp_files, function(f) {
    df <- read_parquet(f)
    missing <- setdiff(shot_cols, names(df))
    if (length(missing) > 0) {
      stop("PBP file ", basename(f), " missing columns: ", paste(missing, collapse = ", "))
    }
    df[, shot_cols]
  }) |> bind_rows()

  # Encode shot outcome: 1 = goal (6 pts), 0 = behind (1 pt), -1 = miss (0 pts)
  pbp |>
    filter(shot_at_goal == TRUE) |>
    transmute(
      player_id,
      season = as.integer(season),
      round_number = as.integer(round_number),
      x = round(x, 1),
      y = round(y, 1),
      distance = round(distance, 1),
      goal_prob = round(goal_prob, 3),
      shot_result = case_when(
        points_shot == 6 ~ 1L,
        points_shot == 1 ~ 0L,
        TRUE             ~ -1L
      ),
      phase_of_play = as.character(phase_of_play),
      venue_length = coalesce(as.integer(venue_length), DEFAULT_VENUE_LENGTH),
      venue_width  = coalesce(as.integer(venue_width), DEFAULT_VENUE_WIDTH)
    )
}

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
if (!is.null(shots)) {
  write_parquet(shots, "blog/torp_shots.parquet")
  cat("torp_shots:", nrow(shots), "shots\n")
}

# Season simulations — Monte Carlo projections (depends on torp package)
# Wrapped in tryCatch so simulation failure doesn't block the core parquets
tryCatch({
  torp_path <- if (dir.exists("../torp")) "../torp" else if (dir.exists("torp")) "torp" else NULL
  if (is.null(torp_path)) stop("torp package not found at ../torp or ./torp")
  devtools::load_all(torp_path, quiet = TRUE)
  library(data.table)

  current_season <- max(preds$season)
  played <- preds$round[preds$season == current_season & !is.na(preds$actual_margin)]
  latest_round <- if (length(played) > 0) max(played) else 0L

  cat("Running", 3000, "season simulations for", current_season,
      "from round", latest_round, "...\n")

  sim_results <- simulate_afl_season(current_season, n_sims = 3000,
                                     seed = 42, verbose = FALSE)
  summary_dt <- summarise_simulations(sim_results)
  n_sims_val <- sim_results$n_sims

  # Finals stage probabilities from raw finals data
  finals <- sim_results$finals
  finals_stage <- finals[, .(
    premiers_pct    = sum(finals_finish == 5) / n_sims_val,
    runner_up_pct   = sum(finals_finish == 4) / n_sims_val,
    lose_prelim_pct = sum(finals_finish == 3) / n_sims_val,
    lose_semi_pct   = sum(finals_finish == 2) / n_sims_val,
    lose_elim_pct   = sum(finals_finish == 1) / n_sims_val
  ), by = team]

  # Position distribution from ladder results
  ladders <- sim_results$ladders
  pos_counts <- ladders[, .N, by = .(team, rank)]
  pos_dist <- dcast(pos_counts, team ~ rank, value.var = "N", fill = 0)
  pos_cols <- as.character(1:18)
  for (col in pos_cols) {
    if (!col %in% names(pos_dist)) pos_dist[, (col) := 0]
    set(pos_dist, j = col, value = pos_dist[[col]] / n_sims_val)
  }
  setnames(pos_dist, pos_cols, paste0("pos_", pos_cols, "_pct"))

  # Normalize sim team names (short) → full AFL names (matching torp_ratings.parquet)
  full_names <- c(
    Adelaide = "Adelaide Crows", `Brisbane Lions` = "Brisbane Lions",
    Carlton = "Carlton", Collingwood = "Collingwood", Essendon = "Essendon",
    Fremantle = "Fremantle", Geelong = "Geelong Cats",
    `Gold Coast` = "Gold Coast SUNS", GWS = "GWS GIANTS",
    Hawthorn = "Hawthorn", Melbourne = "Melbourne",
    `North Melbourne` = "North Melbourne", `Port Adelaide` = "Port Adelaide",
    Richmond = "Richmond", `St Kilda` = "St Kilda",
    Sydney = "Sydney Swans", `West Coast` = "West Coast Eagles",
    Footscray = "Western Bulldogs"
  )
  norm_team <- function(x) {
    mapped <- full_names[x]
    ifelse(is.na(mapped), x, mapped)
  }
  summary_dt[, team := norm_team(team)]
  finals_stage[, team := norm_team(team)]
  pos_dist[, team := norm_team(team)]

  # Current standings from fitzRoy (pre-season = zeros)
  current <- tryCatch({
    ladder <- fitzRoy::fetch_ladder(current_season, source = "AFL")
    latest_rnd <- max(ladder$round_number)
    ladder <- ladder[ladder$round_number == latest_rnd, ]
    data.table(
      team = ladder$team.name,
      current_wins = as.integer(ladder$thisSeasonRecord.winLossRecord.wins),
      current_losses = as.integer(ladder$thisSeasonRecord.winLossRecord.losses),
      current_pct = round(ladder$thisSeasonRecord.percentage, 1)
    )
  }, error = function(e) {
    message("::warning::Could not fetch current ladder (using zeros): ", conditionMessage(e))
    data.table(team = summary_dt$team, current_wins = 0L,
               current_losses = 0L, current_pct = NA_real_)
  })

  # Merge summary + current standings + finals stages + position distribution
  sim_output <- merge(
    summary_dt[, .(team, avg_wins, avg_losses, avg_percentage, avg_rank,
                   top_1_pct, top_4_pct, top_8_pct, last_pct)],
    current, by = "team", all.x = TRUE
  )
  sim_output <- merge(sim_output, finals_stage, by = "team", all.x = TRUE)
  sim_output <- merge(sim_output, pos_dist, by = "team", all.x = TRUE)

  # Fill NAs with 0 for teams that never made finals / pre-season
  pct_cols <- grep("_pct$", names(sim_output), value = TRUE)
  pct_cols <- setdiff(pct_cols, c("avg_percentage", "current_pct"))
  for (col in pct_cols) {
    set(sim_output, which(is.na(sim_output[[col]])), col, 0)
  }
  set(sim_output, which(is.na(sim_output[["current_wins"]])), "current_wins", 0L)
  set(sim_output, which(is.na(sim_output[["current_losses"]])), "current_losses", 0L)

  sim_output[, season := current_season]
  sim_output[, round := latest_round]
  sim_output[, n_sims := n_sims_val]

  write_parquet(as.data.frame(sim_output), "blog/torp_simulations.parquet")
  cat("torp_simulations:", nrow(sim_output), "teams\n")
}, error = function(e) {
  message("::warning::Simulation failed, skipping torp_simulations.parquet: ",
          conditionMessage(e))
})
