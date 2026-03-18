library(arrow)
library(dplyr)

# Player ratings - predictive TORP ratings (career-weighted with exponential decay)
all_ratings <- read_parquet("source/torp_ratings.parquet")

ratings <- all_ratings |>
  filter(season == max(season, na.rm = TRUE)) |>
  filter(round == max(round, na.rm = TRUE)) |>
  select(player_id, player_name, team, position, torp, torp_recv, torp_disp,
         torp_spoil, torp_hitout, gms, season) |>
  arrange(desc(torp))

# Team ratings - most recent round of the most recent season
teams <- read_parquet("source/team_ratings.parquet")
latest_teams <- teams |>
  filter(season == max(season, na.rm = TRUE)) |>
  filter(round == max(as.numeric(round), na.rm = TRUE))

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
# Standardise column names — handle both old camelCase and new snake_case formats.
# Strip 'player.' prefix from legacy flattened AFL API response (no-op for new parquets).
names(details_raw) <- sub("^player\\.", "", names(details_raw))
if ("team.name" %in% names(details_raw) && !"team" %in% names(details_raw)) {
  names(details_raw)[names(details_raw) == "team.name"] <- "team"
}
# Rename old camelCase columns → snake_case (no-op if already snake_case)
old_to_new <- c(
  jumperNumber = "jumper_number", heightInCm = "height_cm", weightInKg = "weight_kg",
  dateOfBirth = "date_of_birth", draftYear = "draft_year", debutYear = "debut_year",
  recruitedFrom = "recruited_from"
)
for (old_nm in names(old_to_new)) {
  if (old_nm %in% names(details_raw) && !old_to_new[[old_nm]] %in% names(details_raw)) {
    names(details_raw)[names(details_raw) == old_nm] <- old_to_new[[old_nm]]
  }
}
# Handle first_name + surname → player_name (new format)
if (!"player_name" %in% names(details_raw) && all(c("first_name", "surname") %in% names(details_raw))) {
  details_raw$player_name <- paste(details_raw$first_name, details_raw$surname)
}
required_detail_cols <- c("player_id", "player_name", "team", "position",
                          "jumper_number", "height_cm", "weight_kg",
                          "date_of_birth", "draft_year", "debut_year",
                          "recruited_from")
missing_detail_cols <- setdiff(required_detail_cols, names(details_raw))
if (length(missing_detail_cols) > 0) {
  stop("player_details parquet missing columns after name standardisation: ",
       paste(missing_detail_cols, collapse = ", "),
       "\nActual columns: ", paste(names(details_raw), collapse = ", "))
}
# season may not be in parquet — derive from filename if absent
if (!"season" %in% names(details_raw)) {
  derived_season <- as.integer(sub(".*player_details_(\\d{4})\\.parquet$", "\\1", details_file))
  if (is.na(derived_season)) {
    stop("Could not derive season from player_details filename: ", basename(details_file),
         "\nExpected pattern: player_details_YYYY.parquet")
  }
  details_raw$season <- derived_season
}
details <- details_raw |>
  transmute(
    player_id,
    player_name,
    team,
    position,
    jumper_number,
    height_cm,
    weight_kg,
    date_of_birth,
    draft_year,
    debut_year,
    recruited_from,
    season
  )

# Game logs - per-game TORP ratings (up to 5 seasons, depending on source data)
game_files <- list.files("source", pattern = "^player_game_ratings_", full.names = TRUE)
if (length(game_files) == 0) stop("No player_game_ratings files found in source/")
game_raw <- lapply(game_files, read_parquet) |> bind_rows()
required_game_cols <- c("player_id", "player_name", "season", "round", "team", "opp",
                        "total_points", "recv_points", "disp_points", "spoil_points",
                        "hitout_points", "match_id")
missing_game_cols <- setdiff(required_game_cols, names(game_raw))
if (length(missing_game_cols) > 0) {
  stop("player_game_ratings parquets missing columns: ",
       paste(missing_game_cols, collapse = ", "))
}
game_logs <- game_raw |>
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

# Raw game stats — box-score stats for match stats toggle (optional)
game_stat_files <- list.files("source", pattern = "^player_game_\\d{4}\\.parquet$", full.names = TRUE)
game_stats <- if (length(game_stat_files) == 0) {
  message("INFO: No player_game files in source/ — skipping torp_game_stats.parquet")
  NULL
} else {
  tryCatch({
    raw <- lapply(game_stat_files, read_parquet) |> bind_rows()
    current_season <- max(raw$season, na.rm = TRUE)
    raw |>
      filter(season >= current_season - 1L) |>
      transmute(
        player_id, player_name, season, round, team, opponent, match_id,
        time_on_ground_percentage,
        # Scoring
        goals, behinds, shots_at_goal, score_involvements, goal_assists, marks_inside50,
        # Possession
        disposals, kicks, handballs, marks,
        uncontested_possessions, clangers, turnovers,
        # Contested
        contested_possessions, contested_marks, ground_ball_gets,
        frees_for, frees_against,
        # Midfield
        clearances, inside50s, rebound50s, bounces, metres_gained,
        # Defense
        tackles, intercepts, one_percenters,
        pressure_acts, def_half_pressure_acts,
        # Ruck
        hitouts, hitouts_to_advantage, ruck_contests
      ) |>
      arrange(player_id, season, round)
  }, error = function(e) {
    message("::warning::Game stats processing failed, skipping torp_game_stats.parquet: ",
            conditionMessage(e))
    NULL
  })
}

# Shot data from PBP — optional, doesn't block core outputs
# Fallback AFL field dimensions (metres) when venue data is absent in PBP.
# 165 x 135 m are typical MCG-class dimensions; actual venues vary.
DEFAULT_VENUE_LENGTH <- 165L
DEFAULT_VENUE_WIDTH  <- 135L

pbp_files <- list.files("source", pattern = "^pbp_data_\\d{4}_all\\.parquet$", full.names = TRUE)
shots <- if (length(pbp_files) == 0) {
  message("INFO: No PBP files in source/ — skipping torp_shots.parquet")
  NULL
} else {
  tryCatch({
    shot_cols <- c("player_id", "season", "round_number", "x", "y", "distance",
                   "goal_prob", "points_shot", "phase_of_play", "venue_length",
                   "venue_width", "shot_at_goal")

    pbp <- lapply(pbp_files, function(f) {
      df <- read_parquet(f, col_select = any_of(shot_cols))
      missing <- setdiff(shot_cols, names(df))
      if (length(missing) > 0) {
        stop("PBP file ", basename(f), " missing columns: ", paste(missing, collapse = ", "))
      }
      df
    }) |> bind_rows()

    # Encode shot outcome from the shooter's perspective:
    #   1L = goal (6 pts)
    #   0L = behind or rushed behind (1 pt — includes both scored and rushedOpp)
    #  -1L = miss or other (0 pts)
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
  }, error = function(e) {
    message("::warning::PBP processing failed, skipping torp_shots.parquet: ",
            conditionMessage(e))
    NULL
  })
}

# Match events from PBP — per-season event-level EPV data for match-events page
# Optional: uses same pbp_files as shots
if (length(pbp_files) > 0) {
  tryCatch({
    event_cols <- c("player_id", "season", "round_number", "period",
                    "period_seconds", "play_type", "phase_of_play",
                    "description", "disposal", "delta_epv", "scored_shot")

    dir.create("blog", showWarnings = FALSE)

    for (pbp_file in pbp_files) {
      pbp_season <- as.integer(sub(".*pbp_data_(\\d{4})_all\\.parquet$", "\\1", basename(pbp_file)))
      pbp <- read_parquet(pbp_file, col_select = any_of(event_cols))

      events <- pbp |>
        filter(!is.na(player_id)) |>
        mutate(
          category = case_when(
            play_type == "Reception" ~ "Ball Winning",
            disposal == "clanger" ~ "Negatives",
            play_type %in% c("Kick", "Handball", "Ground Kick") ~ "Ball Use",
            TRUE ~ NA_character_
          ),
          role = if_else(play_type == "Reception", "receiver", "disposer"),
          detail = description,
          quarter = period,
          time = period_seconds,
          round = round_number,
          equity = delta_epv,
          is_contested = phase_of_play == "Hard Ball",
          is_ineffective = !is.na(disposal) & disposal == "ineffective",
          is_goal = !is.na(scored_shot) & scored_shot == 1,
          is_free_against = FALSE
        ) |>
        filter(!is.na(category)) |>
        arrange(player_id, round, quarter, time) |>
        group_by(player_id, round) |>
        mutate(cumulative_total = cumsum(equity)) |>
        ungroup() |>
        select(player_id, season, round, quarter, time,
               category, detail, equity, cumulative_total,
               is_contested, is_ineffective, is_goal, is_free_against, role)

      out_name <- paste0("match-events-", pbp_season, ".parquet")
      write_parquet(events, file.path("blog", out_name))
      cat(out_name, ":", nrow(events), "events\n")
    }
  }, error = function(e) {
    message("::warning::Match events processing failed: ", conditionMessage(e))
  })
} else {
  message("INFO: No PBP files — skipping match events")
}

# Chain data from PBP — per-season chain action data for match-chains page
# Optional: uses same pbp_files as shots/match-events
if (length(pbp_files) > 0) {
  tryCatch({
    chain_cols <- c("match_id", "chain_number", "display_order", "player_id",
                    "player_name_given_name", "player_name_surname",
                    "team_id", "home_team_id", "home_team_name", "away_team_name",
                    "x", "y", "description", "delta_epv", "disposal",
                    "final_state", "initial_state", "period", "period_seconds",
                    "shot_at_goal", "season", "round_number",
                    "venue_length", "venue_width")

    dir.create("blog", showWarnings = FALSE)

    for (pbp_file in pbp_files) {
      pbp_season <- as.integer(sub(".*pbp_data_(\\d{4})_all\\.parquet$", "\\1", basename(pbp_file)))
      pbp <- read_parquet(pbp_file, col_select = any_of(chain_cols))

      chains <- pbp |>
        filter(!is.na(chain_number)) |>
        transmute(
          match_id,
          chain_number = as.integer(chain_number),
          display_order = as.integer(display_order),
          player_id,
          player_name = trimws(paste(coalesce(player_name_given_name, ""),
                                     coalesce(player_name_surname, ""))),
          team_id,
          home_team_id,
          home_team = home_team_name,
          away_team = away_team_name,
          x = round(x, 1),
          y = round(y, 1),
          description,
          delta_epv = round(delta_epv, 4),
          disposal = as.character(disposal),
          final_state,
          initial_state,
          period = as.integer(period),
          period_seconds = as.integer(period_seconds),
          shot_at_goal = !is.na(shot_at_goal) & shot_at_goal == TRUE,
          season = as.integer(season),
          round_number = as.integer(round_number),
          venue_length = coalesce(as.integer(venue_length), DEFAULT_VENUE_LENGTH),
          venue_width = coalesce(as.integer(venue_width), DEFAULT_VENUE_WIDTH)
        ) |>
        arrange(match_id, chain_number, display_order)

      out_name <- paste0("chains-", pbp_season, ".parquet")
      write_parquet(chains, file.path("blog", out_name))
      cat(out_name, ":", nrow(chains), "chain actions\n")
    }
  }, error = function(e) {
    message("::warning::Chain data processing failed: ", conditionMessage(e))
  })
} else {
  message("INFO: No PBP files — skipping chain data")
}

if (nrow(ratings) <= 100)   stop("ratings has ", nrow(ratings), " rows (expected >100)")
if (nrow(latest_teams) < 18) stop("latest_teams has ", nrow(latest_teams), " rows (expected >=18)")
if (nrow(preds) == 0)        stop("preds is empty — no predictions loaded")
if (nrow(details) == 0)      stop("details is empty — player_details produced no rows")
if (nrow(game_logs) == 0)    stop("game_logs is empty — player_game_ratings produced no rows")

dir.create("blog", showWarnings = FALSE)
write_parquet(ratings, "blog/ratings.parquet")
write_parquet(latest_teams, "blog/team-ratings.parquet")
write_parquet(preds, "blog/predictions.parquet")
write_parquet(details, "blog/player-details.parquet")
write_parquet(game_logs, "blog/game-logs.parquet")
cat("ratings:", nrow(ratings), "players\n")
cat("team-ratings:", nrow(latest_teams), "teams\n")
cat("predictions:", nrow(preds), "matches\n")
cat("player-details:", nrow(details), "players\n")
cat("game-logs:", nrow(game_logs), "game records\n")
if (!is.null(shots)) {
  write_parquet(shots, "blog/shots.parquet")
  cat("shots:", nrow(shots), "shots\n")
}
if (!is.null(game_stats)) {
  write_parquet(game_stats, "blog/game-stats.parquet")
  cat("game-stats:", nrow(game_stats), "game stat records\n")
}

# Season simulations — Monte Carlo projections (depends on torp package)
# Everything inside tryCatch so missing deps do not block the rest of the pipeline.
torp_path <- if (dir.exists("../torp")) "../torp" else if (dir.exists("torp")) "torp" else NULL
if (!is.null(torp_path)) {
  tryCatch({
  devtools::load_all(torp_path, quiet = TRUE)
  library(data.table)

  current_season <- max(preds$season, na.rm = TRUE)
  played <- preds$round[preds$season == current_season & !is.na(preds$actual_margin)]
  latest_round <- if (length(played) > 0) max(played) else 0L

  # Injury data — scrape live and build return schedule for blog
  inj_df <- tryCatch({
    inj <- get_all_injuries(current_season)
    if (nrow(inj) > 0) {
      inj$return_round <- parse_return_round(
        inj$estimated_return, current_season, latest_round
      )
    }
    inj
  }, error = function(e) {
    message("::warning::Injury scrape failed: ", conditionMessage(e))
    NULL
  })

  if (!is.null(inj_df) && nrow(inj_df) > 0) {
    injuries_blog <- inj_df |>
      transmute(
        player = player,
        team = team,
        injury = injury,
        estimated_return = estimated_return,
        return_round = return_round,
        updated = as.character(updated),
        source = source
      ) |>
      arrange(team, return_round)
    write_parquet(as.data.frame(injuries_blog), "blog/injuries.parquet")
    cat("injuries:", nrow(injuries_blog), "players\n")
  }

  cat("Running", 3000, "injury-aware season simulations for", current_season,
      "from round", latest_round, "...\n")

    sim_results <- simulate_afl_season(current_season, n_sims = 3000,
                                     injuries = inj_df,
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

  # Current standings from internal AFL API (pre-season = zeros)
  current <- tryCatch({
    ladder <- get_afl_ladder(current_season)
    if (nrow(ladder) == 0) stop("No ladder data available")
    data.table(
      team = ladder$team,
      current_wins = as.integer(ladder$wins),
      current_losses = as.integer(ladder$losses),
      current_pct = round(ladder$percentage, 1)
    )
  }, error = function(e) {
    message("::warning::Could not compute current ladder (using zeros): ", conditionMessage(e))
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

  write_parquet(as.data.frame(sim_output), "blog/simulations.parquet")
  cat("simulations:", nrow(sim_output), "teams\n")
}, error = function(e) {
  message("::warning::Simulation failed, skipping simulations.parquet: ",
          conditionMessage(e))
})
} else {
  message("INFO: torp package not found — skipping simulations")
}
