library(arrow)
library(dplyr)

# Load torp package for team name normalization (AFL_TEAM_ALIASES + torp_replace_teams)
# Wrapped in tryCatch — missing deps (ggplot2, httr, lubridate) shouldn't block the pipeline
torp_loaded <- FALSE
torp_path <- if (dir.exists("../torp")) "../torp" else if (dir.exists("torp")) "torp" else NULL
if (!is.null(torp_path)) {
  tryCatch({
    suppressMessages(devtools::load_all(torp_path, quiet = TRUE))
    torp_loaded <- TRUE
    cat("Loaded torp package from:", torp_path, "\n")
  }, error = function(e) {
    message("::warning::Could not load torp package: ", conditionMessage(e))
    message("::warning::Team name normalization and simulations will be skipped")
  })
} else {
  message("::warning::torp package not found — team name normalization unavailable")
}

# Player ratings - predictive TORP ratings (career-weighted with exponential decay)
all_ratings <- read_parquet("source/torp_ratings.parquet")

# Handle old column names (torp_recv → recv_epr, etc.)
ratings_renames <- c(torp_recv = "recv_epr", torp_disp = "disp_epr",
                     torp_spoil = "spoil_epr", torp_hitout = "hitout_epr")
for (old_nm in names(ratings_renames)) {
  new_nm <- ratings_renames[[old_nm]]
  if (old_nm %in% names(all_ratings) && !new_nm %in% names(all_ratings)) {
    names(all_ratings)[names(all_ratings) == old_nm] <- new_nm
  }
}

ratings <- all_ratings |>
  select(player_id, player_name, team,
         any_of(c("position_group", "lineup_position", "position")),
         torp, recv_epr, disp_epr,
         spoil_epr, hitout_epr, gms, season, round,
         any_of(c("epr", "psr", "osr", "dsr"))) |>
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

  if ("week" %in% names(pred_raw) && "pred_margin" %in% names(pred_raw)) {
    # Legacy format (2025): flat table with week, pred_margin, pred_xtotal
    # Handle column rename: home_rating → home_epr (torp v2026+)
    if ("home_rating" %in% names(pred_raw)) {
      if ("home_epr" %in% names(pred_raw)) {
        pred_raw$home_epr <- coalesce(pred_raw$home_epr, pred_raw$home_rating)
        pred_raw$away_epr <- coalesce(pred_raw$away_epr, pred_raw$away_rating)
        pred_raw$home_rating <- NULL
        pred_raw$away_rating <- NULL
      } else {
        pred_raw <- pred_raw |> rename(home_epr = home_rating, away_epr = away_rating)
      }
    }
    pred_raw |>
      transmute(
        season = !!season,
        round = week,
        home_team = as.character(home_team),
        away_team = as.character(away_team),
        home_epr = round(home_epr, 1),
        away_epr = round(away_epr, 1),
        pred_margin = round(pred_margin, 1),
        home_win_prob = round(pred_win, 3),
        pred_total = round(pred_xtotal, 0),
        actual_margin = margin,
        start_time = if ("start_time" %in% names(pred_raw)) start_time else NA_character_,
        venue = if ("venue" %in% names(pred_raw)) as.character(venue) else NA_character_
      )
  } else if ("team_type" %in% names(pred_raw) && "pred_score_diff" %in% names(pred_raw)) {
    # Current format (2026+): pivoted table with team_type, pred_score_diff
    pred_raw |>
      filter(team_type == "home") |>
      transmute(
        season = !!season,
        round = round.roundNumber.x,
        home_team = as.character(home_team),
        away_team = as.character(away_team),
        home_epr = round(torp.x, 1),
        away_epr = round(torp.y, 1),
        pred_margin = round(pred_score_diff, 1),
        home_win_prob = round(pred_win, 3),
        pred_total = round(pred_tot_xscore, 0),
        actual_margin = score_diff,
        start_time = if ("start_time" %in% names(pred_raw)) start_time else NA_character_,
        venue = if ("venue" %in% names(pred_raw)) as.character(venue) else NA_character_
      )
  } else {
    warning("Unrecognized predictions format in ", basename(f),
            " — columns: ", paste(head(names(pred_raw), 10), collapse = ", "))
    NULL
  }
})
preds <- bind_rows(preds_list) |>
  mutate(round = as.integer(round)) |>
  arrange(season, round, desc(abs(pred_margin)))

# Backfill with retrodictions for rounds that have no locked predictions
retro_files <- list.files("source", pattern = "^retrodictions_", full.names = TRUE)
if (length(retro_files) > 0) {
  retro_list <- lapply(retro_files, function(f) {
    season <- as.integer(sub(".*retrodictions_(\\d+)\\.parquet$", "\\1", basename(f)))
    r <- read_parquet(f) |> ungroup()
    if (!"week" %in% names(r)) return(NULL)
    if ("home_rating" %in% names(r)) {
      if ("home_epr" %in% names(r)) {
        r$home_epr <- coalesce(r$home_epr, r$home_rating)
        r$away_epr <- coalesce(r$away_epr, r$away_rating)
        r$home_rating <- NULL
        r$away_rating <- NULL
      } else {
        r <- r |> rename(home_epr = home_rating, away_epr = away_rating)
      }
    }
    r |> transmute(
      season = !!season, round = week,
      home_team = as.character(home_team), away_team = as.character(away_team),
      home_epr = round(home_epr, 1), away_epr = round(away_epr, 1),
      pred_margin = round(pred_margin, 1), home_win_prob = round(pred_win, 3),
      pred_total = round(pred_xtotal, 0), actual_margin = margin,
      start_time = if ("start_time" %in% names(r)) start_time else NA_character_,
      venue = if ("venue" %in% names(r)) as.character(venue) else NA_character_
    )
  })
  retro <- bind_rows(retro_list)
  # Only add retrodiction rows for season+round+home+away combos missing from predictions
  retro_new <- retro |>
    anti_join(preds, by = c("season", "round", "home_team", "away_team"))
  if (nrow(retro_new) > 0) {
    preds <- bind_rows(preds, retro_new) |> arrange(season, round, desc(abs(pred_margin)))
    cat("Backfilled", nrow(retro_new), "matches from retrodictions\n")
  }
}

# Normalize team names to canonical full names (e.g., "Adelaide" → "Adelaide Crows")
if (torp_loaded) {
  preds$home_team <- torp_replace_teams(preds$home_team)
  preds$away_team <- torp_replace_teams(preds$away_team)
  cat("Team names normalized:", paste(sort(unique(preds$home_team)), collapse = ", "), "\n")
}

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
required_detail_cols <- c("player_id", "player_name", "team",
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
  select(
    player_id,
    player_name,
    team,
    any_of(c("position_group", "lineup_position", "position")),
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
# Handle column name evolution: total_points → epv_raw → epv (current)
col_renames <- c(
  total_points = "epv", recv_points = "recv_epv", disp_points = "disp_epv",
  spoil_points = "spoil_epv", hitout_points = "hitout_epv",
  epv_adj = "epv", recv_epv_adj = "recv_epv", disp_epv_adj = "disp_epv",
  spoil_epv_adj = "spoil_epv", hitout_epv_adj = "hitout_epv",
  epv_raw = "epv", recv_epv_raw = "recv_epv", disp_epv_raw = "disp_epv",
  spoil_epv_raw = "spoil_epv", hitout_epv_raw = "hitout_epv"
)
for (old_nm in names(col_renames)) {
  new_nm <- col_renames[[old_nm]]
  if (old_nm %in% names(game_raw) && !new_nm %in% names(game_raw)) {
    names(game_raw)[names(game_raw) == old_nm] <- new_nm
  }
}
required_game_cols <- c("player_id", "player_name", "season", "round", "team", "opp",
                        "epv", "recv_epv", "disp_epv", "spoil_epv",
                        "hitout_epv", "match_id")
missing_game_cols <- setdiff(required_game_cols, names(game_raw))
if (length(missing_game_cols) > 0) {
  stop("player_game_ratings parquets missing columns: ",
       paste(missing_game_cols, collapse = ", "))
}
has_psv <- "psv" %in% names(game_raw)
game_logs <- game_raw |>
  mutate(
    torp = epv,
    torp_recv = recv_epv,
    torp_disp = disp_epv,
    torp_spoil = spoil_epv,
    torp_hitout = hitout_epv
  ) |>
  select(player_id, player_name, season, round, team, opp,
         torp, torp_recv, torp_disp, torp_spoil, torp_hitout,
         any_of(c("wp_credit", "wp_disp_credit", "wp_recv_credit")),
         any_of(c("psv", "osv", "dsv")),
         match_id) |>
  arrange(player_id, season, round)

# Join date from fixtures (CI downloads to source/, local dev has them in data/)
fixtures_data_files <- list.files("source", pattern = "^fixtures_.*\\.parquet$", full.names = TRUE)
if (length(fixtures_data_files) == 0) {
  fixtures_data_files <- list.files("data", pattern = "^fixtures_.*\\.parquet$", full.names = TRUE)
}
if (length(fixtures_data_files) > 0) {
  date_lookup <- lapply(fixtures_data_files, function(f) {
    tryCatch(read_parquet(f, col_select = c("match_id", "utc_start_time")), error = function(e) NULL)
  }) |> dplyr::bind_rows()
  if (nrow(date_lookup) > 0 && "utc_start_time" %in% names(date_lookup)) {
    date_lookup <- date_lookup |>
      dplyr::filter(!is.na(utc_start_time), !is.na(match_id)) |>
      dplyr::mutate(
        date = as.character(as.Date(lubridate::with_tz(
          lubridate::ymd_hms(utc_start_time, quiet = TRUE), "Australia/Melbourne")))
      ) |>
      dplyr::select(match_id, date) |>
      dplyr::distinct(match_id, .keep_all = TRUE)
    game_logs <- dplyr::left_join(game_logs, date_lookup, by = "match_id")
    cat("game-logs: date column added from fixtures (",
        sum(!is.na(game_logs$date)), "/", nrow(game_logs), "rows matched)\n")
  } else {
    game_logs$date <- NA_character_
    message("INFO: fixtures parquets lack utc_start_time — date column will be NA")
  }
} else {
  game_logs$date <- NA_character_
  message("INFO: No fixtures parquets in source/ or data/ — date column will be NA")
}

if (has_psv) {
  cat("game-logs: PSV/OSV/DSV columns included\n")
} else {
  message("INFO: PSV/OSV/DSV not in player_game_ratings — upgrade torp to include them")
}
if ("wp_credit" %in% names(game_raw)) {
  cat("game-logs: WPA credit columns included\n")
} else {
  message("INFO: WPA credit not in player_game_ratings — upgrade torp to include them")
}

# Raw game stats — box-score stats for match stats toggle (optional)
game_stat_files <- list.files("source", pattern = "^player_game_\\d{4}\\.parquet$", full.names = TRUE)
game_stats <- if (length(game_stat_files) == 0) {
  message("INFO: No player_game files in source/ — skipping torp_game_stats.parquet")
  NULL
} else {
  tryCatch({
    raw <- lapply(game_stat_files, read_parquet) |> bind_rows()
    raw |>
      select(
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
        hitouts, hitouts_to_advantage, ruck_contests,
        # Efficiency
        any_of(c("effective_disposals", "effective_kicks",
                 "disposal_efficiency", "kick_efficiency"))
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
    shot_cols <- c("match_id", "team_id", "home_team_id",
                   "home_team_name", "away_team_name",
                   "player_id", "season", "round_number",
                   "x", "y", "distance",
                   "goal_prob", "behind_prob", "xscore", "points_shot",
                   "phase_of_play", "venue_length", "venue_width", "shot_at_goal")

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
    # Extract match-level home/away mapping before filtering to shots (for xScore enrichment)
    # One row per match — use distinct on match_id to prevent fan-out from NA home_team_id
    pbp_match_home <- pbp |>
      filter(!is.na(home_team_id)) |>
      distinct(match_id, .keep_all = TRUE) |>
      select(match_id, home_team_id, home_team_name, away_team_name) |>
      mutate(
        home_team_name = if (torp_loaded) torp_replace_teams(home_team_name) else home_team_name,
        away_team_name = if (torp_loaded) torp_replace_teams(away_team_name) else away_team_name
      )

    pbp |>
      filter(shot_at_goal == TRUE) |>
      transmute(
        match_id,
        team_id,
        player_id,
        season = as.integer(season),
        round_number = as.integer(round_number),
        x = round(x, 1),
        y = round(y, 1),
        distance = round(distance, 1),
        goal_prob = round(goal_prob, 3),
        behind_prob = round(behind_prob, 3),
        xscore = round(xscore, 2),
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

# Enrich predictions with match-level xScore from shots
if (!is.null(shots) && "match_id" %in% names(shots) && "team_id" %in% names(shots)) {
  tryCatch({
    # Per-match, per-team xScore totals
    team_xs <- shots |>
      filter(!is.na(xscore)) |>
      group_by(match_id, team_id, season, round_number) |>
      summarise(xscore = round(sum(xscore, na.rm = TRUE), 1), .groups = "drop")

    # Use pre-extracted match_home from PBP read (avoids re-reading large files)
    match_home <- pbp_match_home

    # Tag each team's shots as home or away, pivot to one row per match
    match_xs <- team_xs |>
      inner_join(match_home, by = "match_id") |>
      mutate(position = if_else(team_id == home_team_id, "home", "away")) |>
      select(season, round = round_number, home_team = home_team_name,
             away_team = away_team_name, position, xscore) |>
      tidyr::pivot_wider(id_cols = c(season, round, home_team, away_team),
                         names_from = position, values_from = xscore,
                         names_prefix = "xscore_")

    # Cast round to integer to match predictions
    match_xs$round <- as.integer(match_xs$round)

    # Join with predictions
    preds <- preds |>
      select(-any_of(c("xscore_home", "xscore_away"))) |>
      left_join(match_xs, by = c("season", "round", "home_team", "away_team"))

    n_xs <- sum(!is.na(preds$xscore_home))
    n_miss <- nrow(preds[!is.na(preds$actual_margin) & is.na(preds$xscore_home), ])
    cat("predictions xScore enrichment:", n_xs, "/", nrow(preds), "matches")
    if (n_miss > 0) cat(" (", n_miss, "played matches missing xScore)")
    cat("\n")
  }, error = function(e) {
    message("::warning::xScore enrichment failed: ", conditionMessage(e))
  })
}

# Player finishing skill — per-player random effects from the shot GAM
shot_mdl_path <- "source/shot_ocat_mdl.rds"
if (torp_loaded && file.exists(shot_mdl_path)) {
  # mgcv must be attached so stats::coef/vcov dispatch to coef.gam/vcov.gam.
  # Loaded outside the tryCatch so a missing install fails loudly rather than
  # masquerading as a missing optional input.
  suppressPackageStartupMessages(library(mgcv))
  tryCatch({
    shot_mdl <- readRDS(shot_mdl_path)
    finishing <- extract_player_xg_skill(shot_model = shot_mdl) |> as.data.frame()
    if (!is.null(finishing) && nrow(finishing) > 0) {
      # Backfill names from training-time mapping (covers retired players that
      # load_player_details(current season) misses)
      player_df_path <- "source/shot_player_df.rds"
      if (file.exists(player_df_path)) {
        name_lookup <- readRDS(player_df_path) |>
          transmute(player_id = as.character(player_id_shot),
                    player_name_train = player_name_shot)
        finishing <- finishing |>
          left_join(name_lookup, by = "player_id") |>
          mutate(player_name = coalesce(player_name, player_name_train)) |>
          select(-player_name_train)
      }
      finishing_blog <- finishing |>
        filter(player_id != "Other", !is.na(player_id)) |>
        transmute(
          player_id,
          player_name,
          xg_skill = round(xg_skill, 4),
          xg_skill_se = round(xg_skill_se, 4),
          n_shots = as.integer(n_shots)
        ) |>
        arrange(desc(xg_skill))
      # Guard against a duplicated player_id in shot_player_df.rds fanning out
      # the join and shipping duplicates to R2
      stopifnot(!anyDuplicated(finishing_blog$player_id))
      dir.create("blog", showWarnings = FALSE)
      write_parquet(finishing_blog, "blog/player-finishing.parquet")
      cat("player-finishing:", nrow(finishing_blog), "players\n")
    } else {
      message("INFO: extract_player_xg_skill returned no rows — skipping player-finishing.parquet")
    }
  }, error = function(e) {
    message("::warning::Player finishing extraction failed: ", conditionMessage(e))
  })
} else {
  message("INFO: Shot model or torp not available — skipping player-finishing.parquet")
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
                    "x", "y", "description", "delta_epv", "wpa", "disposal",
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
          wpa = round(wpa, 4),
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

# Fixtures history — venue/date/scores for all seasons (blog stats filters)
fixtures_files <- list.files("source", pattern = "^fixtures_", full.names = TRUE)
if (length(fixtures_files) > 0) {
  tryCatch({
    fixtures_raw <- lapply(fixtures_files, read_parquet) |> dplyr::bind_rows()
    # Team/venue names in fixtures_*.parquet are already normalized at scrape time
    # by load_fixtures() → .normalise_fixture_columns() in torp package.
    # Use torp_replace_teams/venues if available, otherwise trust source names.
    norm_team  <- if (torp_loaded)  torp_replace_teams  else identity
    norm_venue <- if (exists("torp_replace_venues")) torp_replace_venues else identity
    fixtures_blog <- fixtures_raw |>
      dplyr::filter(!is.na(round_number)) |>
      dplyr::transmute(
        match_id    = match_id,
        season      = as.integer(season),
        round       = as.integer(round_number),
        home_team   = norm_team(home_team_name),
        away_team   = norm_team(away_team_name),
        venue       = if ("venue_name" %in% names(fixtures_raw)) norm_venue(venue_name) else NA_character_,
        start_time  = if ("utc_start_time" %in% names(fixtures_raw)) utc_start_time else NA_character_,
        home_score  = as.integer(home_score),
        away_score  = as.integer(away_score),
        status      = status
      ) |>
      dplyr::arrange(season, round)
    write_parquet(fixtures_blog, "blog/fixtures-history.parquet")
    cat("fixtures-history:", nrow(fixtures_blog), "matches\n")
  }, error = function(e) {
    message("::warning::Fixtures processing failed, skipping: ", conditionMessage(e))
  })
} else {
  message("INFO: No fixtures files in source/ — skipping fixtures-history.parquet")
}

# Season simulations — Monte Carlo projections (depends on torp package)
# Everything inside tryCatch so missing deps do not block the rest of the pipeline.
# torp is already loaded at the top of the script (line 5-11) — skip redundant load_all
if (torp_loaded) {
  tryCatch({
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

  # Normalize sim team names → canonical full names (matching torp_ratings.parquet)
  summary_dt[, team := torp_replace_teams(team)]
  finals_stage[, team := torp_replace_teams(team)]
  pos_dist[, team := torp_replace_teams(team)]

  # Current standings from internal AFL API (pre-season = zeros)
  current <- tryCatch({
    ladder <- get_afl_ladder(current_season)
    if (nrow(ladder) == 0) stop("No ladder data available")
    data.table(
      team = torp_replace_teams(ladder$team),
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
  message("::warning::torp package not loaded — skipping simulations")
}
