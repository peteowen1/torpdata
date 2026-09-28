#!/usr/bin/env Rscript
# Build afl/chains-{season}.parquet — per-row chain/PBP data enriched with
# per-row WP and WPA-credit-split columns, for the blog's AFL Match Stats
# Value tab per-quarter WPA toggle (docs/plans/AFL_CHAIN_PARQUET_PLAN.md,
# Stage 2). Stage 1 (torp PR #114) shipped attach_per_row_wpa_split(), whose
# per-row wpa_disp/wpa_recv reproduce create_wp_credit() exactly.
#
# IMPORTANT — this file already exists in production and is read by TWO live
# blog features that predate this plan: afl/match-chains.qmd (the chain
# visualizer — reads x/y/disposal/team names for the pass diagram) and the
# Pass Map section of afl/match.qmd (same x/y/disposal/team-name columns).
# Those features are NOT in the plan's target column table (which explicitly
# excludes x/y "the chain viz already gets these from the worker live-chains
# endpoint if needed" — that assumption doesn't hold; verified 2026-07-21 by
# grepping both .qmd files, which fetch x/y directly from THIS file). So this
# script emits the plan's target columns PLUS the extra columns those two
# pages already depend on (x, y, disposal, initial_state, home_team_id,
# home_team, away_team) — dropping them would silently break both pages.
# The old "Chain data from PBP" block in build_blog_data.R (which used to be
# the sole writer of this file, with a narrower ad-hoc column set and no
# WP/WPA-split columns) is removed in the same commit as this script, so
# there is exactly one writer of blog/chains-{season}.parquet.
#
# BACKFILL GOTCHA — check before adding any NEW column to PBP_COLS below.
# load_pbp()'s column selection uses dplyr::any_of(), which SILENTLY DROPS a
# requested column that a given season's released parquet does not have. The
# bare-symbol projection at the bottom of this script then hard-crashes on that
# season with "object not found" rather than degrading. So a column is only safe
# to add here once every pbp_data_{season}_all.parquet in the release has it --
# which means after the historical rebuild that regenerated them, not merely
# after the torp code that computes it. Verified 2026-09-04 for coord_team_id /
# coord_home_team_id / the five contest_* columns: all present in 2021 through
# 2026, so `Rscript build_afl_chains.R 2021 ... 2025` works today.
#
# SECOND OUTPUT, chain-events-{season}.parquet (added 2026-09-27): every event
# in torp's raw chains, not only the rows the EPV model keeps. It replaces this
# file once its readers have moved over (afl/match-chains, afl/match pass map,
# afl/player heatmap, afl/team pass network, afl/topv-leaderboard, and the
# all-australian-topv-squad blog post); then the chains-{season} write below is
# deleted. Do not add columns to chains-{season} in the meantime.
#
# Usage:
#   Rscript scripts/build_afl_chains.R            # current season only
#   Rscript scripts/build_afl_chains.R 2024 2025   # backfill specific seasons

suppressMessages(library(arrow))
suppressMessages(library(data.table))

torp_path <- if (dir.exists("../torp")) "../torp" else if (dir.exists("torp")) "torp" else NULL
if (is.null(torp_path)) {
  stop("torp package not found (looked for ../torp and torp) — cannot build chain parquet")
}
suppressMessages(devtools::load_all(torp_path, quiet = TRUE))
source("scripts/parquet_helpers.R")  # write_parquet_grouped()
# chain-events needs load_score_events() (torp 78fa65b3). Checked here, not
# inside the per-season tryCatch, so a torp checkout that is too old stops
# the build instead of quietly writing no chain-events files.
if (!exists("load_score_events", mode = "function")) {
  stop("torp is too old for chain-events: load_score_events() not found (needs torp 78fa65b3 or later)")
}
cat("Loaded torp package from:", torp_path, "\n")

args <- commandArgs(trailingOnly = TRUE)
seasons <- if (length(args) > 0) as.integer(args) else get_afl_season()
cat("Building chain parquet(s) for season(s):", paste(seasons, collapse = ", "), "\n")

dir.create("blog", showWarnings = FALSE)

# Columns read from load_pbp() — the plan's target columns, plus utc_start_time
# and team (only needed transiently for the create_wp_credit() verification
# check below, not written to the output parquet), plus the extra columns
# match-chains.qmd / match.qmd's pass map still depend on (see header note).
PBP_COLS <- c(
  "match_id", "season", "round_number", "display_order", "period", "period_seconds",
  "chain_number", "description", "shot_at_goal", "final_state", "initial_state",
  "team_id", "player_id", "player_name", "lead_player_id", "pos_team",
  "wp", "wpa", "delta_epv", "x", "y", "disposal",
  "home_team_id", "home_team_name", "away_team_name",
  "team", "utc_start_time",
  # torpdata#85: the chain-level possessing team. NOT the frame of (x, y) in
  # chains-{season}: the play-by-play's x, y are in each row's ACTOR's frame
  # (team_id), measured 2026-09-27 -- read in coord_team_id's frame, the
  # implied kick lengths come out median 24.7 m with 14.1% over 80 m, against
  # 21.2 m and 2.9% in the actor's frame. Team-less rows (centre bounce,
  # ball-up, out of bounds) copy the NEXT actor's frame. chain-events-{season}
  # below has one frame per row instead (frame_team_id).
  "coord_team_id", "coord_home_team_id",
  # torpdata#82: aerial-contest detail. The Spoil / Contest Target ROWS are
  # dropped upstream by EPV_RELEVANT_DESCRIPTIONS (clean_features.R) before
  # this script ever runs, but add_contest_vars_dt() (clean_pbp.R) deliberately
  # collapses their information onto the preceding Kick row first -- so the
  # duel is recoverable from these columns without touching that whitelist.
  # Widening the whitelist instead would be a rating change, not an additive
  # one: the filter runs BEFORE the lag/lead features are built, so restoring
  # rows shifts every neighbouring row's "previous event" and moves delta_epv,
  # player credit, and published EPR. Measured 2026-09-04, hence this route.
  "contest_target_id", "contest_target_team_id",
  "contest_defender_id", "contest_defender_team_id", "contest_outcome",
  # chain-events only: expected score before the action, and whose it is
  # (torp add_epv_vars(): exp_pts is in team_id_mdl's frame, torp#217).
  # Verified 2026-09-27 present in every season 2021 through 2026 (a full
  # `Rscript build_afl_chains.R 2021 ... 2026` run loaded both).
  "exp_pts", "team_id_mdl"
)

# Final output column order — plan's target table first, then the extras
# kept for match-chains.qmd / match.qmd pass-map compatibility (see header).
OUTPUT_COLS <- c(
  "match_id", "season", "round_number", "display_order", "period", "period_seconds",
  "chain_number", "description", "shot_at_goal", "final_state",
  "team_id", "player_id", "player_name", "lead_player_id", "pos_team",
  "wp", "wpa", "wpa_disp", "wpa_recv", "delta_ep", "player_credit",
  # extras (existing chain-viz / pass-map consumers)
  "x", "y", "disposal", "initial_state", "home_team_id", "home_team", "away_team",
  # torpdata#85: coordinate-frame team, so the blog can orient (x, y) without
  # a per-page heuristic
  "coord_team_id", "coord_home_team_id",
  # torpdata#82: aerial-contest detail, so the contest boards are computable
  # client-side (populated on contest Kick rows only -- ~1.7% of rows)
  "contest_target_id", "contest_target_team_id",
  "contest_defender_id", "contest_defender_team_id", "contest_outcome"
)

for (season in seasons) {
  cat("\n=== Season", season, "===\n")

  pbp_raw <- load_pbp(season, rounds = TRUE, columns = PBP_COLS)
  if (nrow(pbp_raw) == 0) {
    message("::warning::No PBP data for season ", season, " — skipping")
    next
  }
  pbp <- data.table::as.data.table(pbp_raw)
  n_matches <- length(unique(pbp$match_id))
  cat("Loaded pbp:", nrow(pbp), "rows,", n_matches, "matches\n")

  required_cols <- c("wpa", "player_id", "player_name", "lead_player_id", "pos_team",
                     "display_order", "match_id", "team", "utc_start_time",
                     "round_number", "description", "delta_epv", "wp")
  missing_cols <- setdiff(required_cols, names(pbp))
  if (length(missing_cols) > 0) {
    stop("Season ", season, ": pbp missing required columns: ",
         paste(missing_cols, collapse = ", "))
  }

  # Reuse pre-computed wp/wpa from the released pbp (add_wp_vars() already ran
  # upstream in torp's daily pipeline before the pbp-data release was written)
  # rather than recomputing the model. Only fall back to add_wp_vars() if the
  # loaded data somehow lacks it (defensive — shouldn't happen for released data).
  if (all(is.na(pbp$wp)) || all(is.na(pbp$wpa))) {
    cat("wp/wpa missing or all-NA in loaded pbp — running add_wp_vars()\n")
    pbp <- data.table::as.data.table(add_wp_vars(pbp))
  } else {
    cat("Reusing pre-computed wp/wpa columns from the released pbp (no model recompute)\n")
  }

  # Stage 1 helper (torp PR #114): per-row disposer/receiver WPA split,
  # verified below to reproduce create_wp_credit()'s per-player totals exactly.
  pbp <- attach_per_row_wpa_split(pbp, disp_share = WP_CREDIT_DISP_SHARE)

  # --- player_credit / delta_ep -------------------------------------------
  # delta_ep is torp's existing per-row EPV delta (add_epv_vars()' `delta_epv`,
  # just exported under the blog/worker-facing name). player_credit mirrors
  # the DISPOSER half of the same 50/50 split attach_per_row_wpa_split() does
  # for WPA, applied to delta_epv instead of wpa (same has_receiver
  # conditional and defensive Goal/Behind/Rushed exclusion) — torp has no
  # exported per-row EPV-credit-split helper (Stage 1 only covered WPA; the
  # real disp_epv/recv_epv computation in player_credit.R needs a
  # player_stats/box-score join that isn't available from PBP rows alone), so
  # this is computed locally here rather than sourced from torp.
  pbp[, .has_recv_ep := !is.na(lead_player_id) & lead_player_id != player_id]
  pbp[, player_credit := data.table::fifelse(
    .has_recv_ep, WP_CREDIT_DISP_SHARE * delta_epv, delta_epv
  )]
  excluded_ep <- is.na(pbp$player_id) | is.na(pbp$delta_epv) |
    pbp$description %in% c("Goal", "Behind", "Rushed")
  pbp[excluded_ep, player_credit := NA_real_]
  pbp[, .has_recv_ep := NULL]

  data.table::setnames(pbp, "delta_epv", "delta_ep")

  # ---- Sanity check 1: per-player identity vs create_wp_credit() ----------
  verify_match <- pbp[, .N, by = match_id][order(-N)][1, match_id]
  cat("Identity check match_id:", verify_match, "\n")

  verify_pbp <- pbp[match_id == verify_match]
  # create_wp_credit() needs `team`/`utc_start_time`/`round_number` which are
  # present pre-projection (dropped from the final parquet below).
  wpc <- create_wp_credit(verify_pbp)

  disp_sum <- verify_pbp[, .(disp_total = sum(wpa_disp, na.rm = TRUE)), by = player_id]
  recv_sum <- verify_pbp[!is.na(wpa_recv) & wpa_recv != 0,
                         .(recv_total = sum(wpa_recv, na.rm = TRUE)), by = lead_player_id]
  data.table::setnames(recv_sum, "lead_player_id", "player_id")
  combined <- merge(disp_sum, recv_sum, by = "player_id", all = TRUE)
  combined[is.na(disp_total), disp_total := 0]
  combined[is.na(recv_total), recv_total := 0]
  combined[, wp_credit_check := disp_total + recv_total]

  # combined can contain players create_wp_credit() never sees at all (e.g. a
  # player whose ONLY chain row is an excluded descriptive Goal/Behind/Rushed
  # scoring row -- wpa_disp/wpa_recv are NA there, so their sum() is 0, but
  # create_wp_credit()'s dt[] filter drops that row before aggregating, so
  # the player never appears in disp_agg at all). That's not a disagreement
  # -- both sides agree the player earns zero credit -- so compare on wpc's
  # player set (all.x) and separately assert no combined-only player carries
  # a nonzero credit (which WOULD indicate a real split-logic bug).
  extra <- combined[!wpc, on = "player_id"]
  extra_nonzero <- extra[abs(wp_credit_check) > 1e-9]
  if (nrow(extra_nonzero) > 0) {
    stop("Season ", season, ": identity check FAILED for match ", verify_match,
         " — ", nrow(extra_nonzero), " player(s) have nonzero per-row credit ",
         "but are absent from create_wp_credit() entirely")
  }
  cmp <- merge(wpc[, .(player_id, wp_credit)], combined[, .(player_id, wp_credit_check)],
              by = "player_id", all.x = TRUE)
  cmp[is.na(wp_credit_check), wp_credit_check := 0]
  max_diff <- max(abs(cmp$wp_credit - cmp$wp_credit_check))
  cat("create_wp_credit() vs per-row wpa_disp+wpa_recv sum — max abs diff:",
      max_diff, "(", nrow(cmp), "players,", nrow(extra), "zero-credit-only players excluded )\n")
  if (nrow(cmp) != nrow(wpc)) {
    stop("Season ", season, ": identity check player-count mismatch (",
         nrow(cmp), " vs ", nrow(wpc), ") for match ", verify_match)
  }
  if (max_diff > 1e-6) {
    stop("Season ", season, ": identity check FAILED for match ", verify_match,
         " — per-row wpa_disp/wpa_recv sums do not reproduce create_wp_credit() ",
         "(max abs diff ", max_diff, ")")
  }
  cat("Identity check PASSED\n")

  # ---- Sanity check 2: Q1+Q2+Q3+Q4 == All per player (holds by construction,
  # since period is an exhaustive/mutually-exclusive partition of the rows) ---
  disp_by_q <- verify_pbp[!is.na(wpa_disp),
                          .(q_total = sum(wpa_disp, na.rm = TRUE)), by = .(player_id, period)]
  disp_q_sum <- disp_by_q[, .(q_sum = sum(q_total)), by = player_id]
  disp_all <- verify_pbp[, .(all_total = sum(wpa_disp, na.rm = TRUE)), by = player_id]
  q_cmp <- merge(disp_q_sum, disp_all, by = "player_id", all = TRUE)
  q_cmp[is.na(q_sum), q_sum := 0]
  q_cmp[is.na(all_total), all_total := 0]
  q_max_diff <- max(abs(q_cmp$q_sum - q_cmp$all_total))
  cat("Q1+Q2+Q3+Q4 vs All (wpa_disp) — max abs diff:", q_max_diff, "\n")
  if (q_max_diff > 1e-9) {
    stop("Season ", season, ": Q1+Q2+Q3+Q4 != All for match ", verify_match,
         " (max abs diff ", q_max_diff, ") — period partition is broken")
  }
  cat("Quarter-sum identity check PASSED\n")

  # ---- Project to output schema -------------------------------------------
  out <- pbp[, .(
    match_id,
    season = as.integer(season),
    round_number = as.integer(round_number),
    display_order = as.integer(display_order),
    period = as.integer(period),
    period_seconds = as.integer(period_seconds),
    chain_number = as.integer(chain_number),
    description,
    shot_at_goal = !is.na(shot_at_goal) & shot_at_goal == TRUE,
    final_state,
    team_id,
    player_id,
    player_name,
    lead_player_id,
    pos_team,
    wp = round(wp, 4),
    wpa = round(wpa, 4),
    wpa_disp = round(wpa_disp, 4),
    wpa_recv = round(wpa_recv, 4),
    delta_ep = round(delta_ep, 4),
    player_credit = round(player_credit, 4),
    x = round(x, 1),
    y = round(y, 1),
    disposal,
    initial_state,
    home_team_id,
    home_team = home_team_name,
    away_team = away_team_name,
    coord_team_id,
    coord_home_team_id,
    contest_target_id,
    contest_target_team_id,
    contest_defender_id,
    contest_defender_team_id,
    contest_outcome
  )]
  data.table::setcolorder(out, OUTPUT_COLS)

  out_file <- file.path("blog", paste0("chains-", season, ".parquet"))
  arrow::write_parquet(as.data.frame(out), out_file)
  cat("Wrote", out_file, ":", nrow(out), "rows,", n_matches, "matches (",
      round(file.info(out_file)$size / 1024^2, 2), "MB )\n")

  # ==== chain-events-{season}.parquet ========================================
  # Every event in the raw chains feed: the play-by-play above keeps only the
  # rows the EPV model uses (2026: 363,145 of 447,945), dropping Goal / Behind
  # / Rushed rows, spoils, contest targets, Kick Into F50 and the rest. The two
  # join 1:1 on (match_id, display_order).
  #
  # Coordinates: x, y are metres from the centre of the ground in the frame of
  # frame_team_id (the raw feed's chain_team_id), on EVERY row including
  # centre bounces, ball-ups and out-of-bounds rows, which carry no team of
  # their own. x > 0 is the end frame_team_id attacks. Drawn with that team
  # attacking to the right, y > 0 is the TOP of the ground (checked against a
  # known moment: Logan Morris's winning goal in the 2026 Grand Final was
  # kicked from the top side). The ground is venue_length x venue_width
  # metres. (chains-{season} is different: each row there is in its ACTOR's
  # frame, team_id, which is why team-less rows in it have no frame at all.)
  #
  # Model columns (exp_pts, delta_ep, wp, wpa, credits) are joined from the
  # play-by-play and are NA -- never 0 -- on rows the model does not use.
  # exp_pts is the expected score BEFORE the action, for ep_team_id (the team
  # in possession: on a stoppage that is the side that wins it, which is not
  # always frame_team_id). After the action is exp_pts + delta_ep.
  # Until chain-events has readers, a failure here warns (loudly, as a CI
  # annotation) and moves on: chains-{season} above is already written and is
  # what five blog pages read. Make this a hard failure when chain-events
  # replaces chains-{season}.
  tryCatch({
  raw <- data.table::as.data.table(load_chains(season, rounds = TRUE))
  if (nrow(raw) == 0) stop("load_chains() returned no rows")
  raw[, `:=`(display_order = as.integer(display_order), round_number = as.integer(round_number))]
  if (anyDuplicated(raw, by = c("match_id", "display_order"))) {
    stop("Season ", season, ": raw chains have duplicate (match_id, display_order) keys")
  }
  if (anyNA(raw$chain_team_id)) {
    stop("Season ", season, ": ", sum(is.na(raw$chain_team_id)), " raw rows have no chain_team_id, ",
         "so they have no coordinate frame")
  }
  for (col in c("exp_pts", "team_id_mdl")) {
    if (!col %in% names(pbp)) stop("Season ", season, ": pbp release lacks ", col, " (see BACKFILL GOTCHA)")
  }

  model <- pbp[, .(match_id, display_order = as.integer(display_order),
                   ep_team_id = team_id_mdl, exp_pts, delta_ep, wp, wpa, wpa_disp, wpa_recv,
                   player_credit, lead_player_id, pos_team,
                   contest_target_id, contest_target_team_id, contest_defender_id,
                   contest_defender_team_id, contest_outcome,
                   .pbp_x = x, .pbp_y = y, .pbp_team = team_id, .pbp_desc = description)]
  if (anyDuplicated(model, by = c("match_id", "display_order"))) {
    stop("Season ", season, ": pbp has duplicate (match_id, display_order) keys")
  }
  ev <- merge(raw, model, by = c("match_id", "display_order"), all.x = TRUE, sort = FALSE)
  if (nrow(ev) != nrow(raw)) stop("Season ", season, ": join changed the row count (", nrow(raw), " -> ", nrow(ev), ")")
  n_joined <- sum(!is.na(ev$.pbp_desc))
  if (n_joined != nrow(pbp)) {
    stop("Season ", season, ": only ", n_joined, " of ", nrow(pbp),
         " play-by-play rows found a raw event on (match_id, display_order)")
  }
  bad_desc <- ev[!is.na(.pbp_desc) & .pbp_desc != description, .N]
  if (bad_desc > 0) stop("Season ", season, ": ", bad_desc, " joined rows disagree on description")

  # Frame check: the play-by-play's x is in the actor's frame, so on a row
  # whose actor is the chain team it equals raw x, and on an opponent's row it
  # is raw x mirrored. Team-less rows copy the next actor's frame, so they
  # are left out. Anything short of near-total agreement means the raw frame
  # is not what the note above says.
  fc <- ev[!is.na(.pbp_x) & !is.na(.pbp_team) & !is.na(team_id) & .pbp_team == team_id]
  fc_ok <- fc[, abs(data.table::fifelse(team_id == chain_team_id, x, -x) - .pbp_x) <= 1]
  cat("Frame check: raw x in chain_team_id's frame on", sum(fc_ok), "of", length(fc_ok), "rows\n")
  if (length(fc_ok) == 0 || mean(fc_ok) < 0.999) {
    stop("Season ", season, ": frame check failed (", round(100 * mean(fc_ok), 2), "% agree)")
  }
  # y sign: the raw feed's y is the NEGATIVE of the play-by-play's (torp
  # flips it when cleaning; 1,265 of 1,265 Grand Final rows, 2026-09-28).
  # chain-events publishes the play-by-play's sign, so y > 0 is the top of
  # the ground as documented above. Missed once: the first chain-events
  # checked x only, and the blog drew the Grand Final winner on the wrong
  # side. Checked on every actor row, mirrored like x: the play-by-play is in
  # the actor's frame, which for an opponent's action is the frame rotated
  # 180 degrees, so both axes flip. Actor = frame team: pbp y = -raw y;
  # opponent actor: pbp y = raw y.
  fy <- fc[!is.na(.pbp_y)]
  fy_ok <- fy[, abs(data.table::fifelse(team_id == chain_team_id, -y, y) - .pbp_y) <= 1]
  cat("y check: raw y, sign-corrected and mirrored like x, equals the play-by-play's y on",
      sum(fy_ok), "of", length(fy_ok), "rows\n")
  if (length(fy_ok) == 0 || mean(fy_ok) < 0.999) {
    stop("Season ", season, ": y sign check failed (", round(100 * mean(fy_ok), 2), "% agree)")
  }

  # Chain-boundary repeats: the feed lists the event at every chain boundary
  # twice, as the last row of one chain and the first row of the next (same
  # action, player, team and second). About 23,000 rows a season (5.5%). Kept,
  # so the rows still join 1:1 to the pbp, but flagged so a replay or a count
  # can skip the copy.
  data.table::setorder(ev, match_id, display_order)
  same <- function(a, b) (is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & a == b)
  ev[, repeat_of_prev := chain_number != data.table::shift(chain_number) &
       same(description, data.table::shift(description)) & period == data.table::shift(period) &
       same(period_seconds, data.table::shift(period_seconds)) &
       same(team_id, data.table::shift(team_id)) & same(player_id, data.table::shift(player_id)),
     by = match_id]
  ev[is.na(repeat_of_prev), repeat_of_prev := FALSE]

  # Running score, from the AFL API's own list of every scoring event
  # (torp::load_score_events(), score_events-data release). The chains feed
  # cannot give it: most rushed behinds have no row, a chain that ended in one
  # can be labelled outOfBounds, and a kick in flight at the siren leaves
  # nothing (rebuilt from the chains it matched the final in 1,275 of 1,279
  # matches, 2021-2026). Each event is attached to one row: the Goal / Behind /
  # Rushed row of that type within 5 seconds if there is one, otherwise the
  # last row at or before its time in that quarter. home_score / away_score
  # are the API's running score after that row.
  se <- data.table::as.data.table(load_score_events(season))
  if (nrow(se) == 0) stop("no score events for ", season, " (torpdata score_events-data release)")
  se <- se[, .(match_id, period = as.integer(period_number), t = as.integer(period_seconds),
               score_type, score_team_id = team_id, score_player_id = player_score_player_player_id,
               agg_home = as.integer(aggregate_home_score), agg_away = as.integer(aggregate_away_score),
               event_number = as.integer(event_number))]
  se[, row_desc := data.table::fcase(score_type == "GOAL", "Goal", score_type == "BEHIND", "Behind",
                                     score_type == "RUSHED_BEHIND", "Rushed", default = NA_character_)]
  rows <- ev[, .(match_id, period, t = period_seconds, display_order, description)]
  # 1. a scoring row of the same type, nearest in time (within 5 s)
  typed <- rows[description %in% c("Goal", "Behind", "Rushed")]
  data.table::setnames(typed, "description", "row_desc")
  typed[, t_row := t]
  data.table::setkey(typed, match_id, period, row_desc, t)
  hit <- typed[se, on = .(match_id, period, row_desc, t), roll = "nearest", mult = "first",
               .(event_number = i.event_number, match_id, display_order, gap = abs(t_row - i.t))]
  se[hit, on = .(match_id, event_number), `:=`(at_order = data.table::fifelse(i.gap <= 5, i.display_order, NA_integer_))]
  # 2. otherwise the last row at or before the event in that quarter
  data.table::setkey(rows, match_id, period, t, display_order)
  prior <- rows[se[is.na(at_order)], on = .(match_id, period, t), roll = Inf, mult = "last",
                .(event_number = i.event_number, match_id, display_order)]
  se[prior, on = .(match_id, event_number), at_order := data.table::fifelse(is.na(at_order), i.display_order, at_order)]

  unplaced <- se[is.na(at_order)]
  no_rows <- setdiff(unique(se$match_id), unique(ev$match_id))
  unplaced <- unplaced[!match_id %in% no_rows]
  cat("Score events placed on a row:", nrow(se) - nrow(unplaced) - se[match_id %in% no_rows, .N], "of",
      se[!match_id %in% no_rows, .N], "\n")
  if (length(no_rows)) {
    message("::warning::chain-events-", season, ": ", length(no_rows), " match(es) have scores but no chain rows: ",
            paste(no_rows, collapse = ", "))
  }
  if (nrow(unplaced) > 0) {
    # A cut-off chains feed (the quarter has no rows) leaves its later scores
    # unplaced; that match's running score then stops where its rows stop.
    message("::warning::chain-events-", season, ": ", nrow(unplaced), " score event(s) had no row to sit on, in ",
            data.table::uniqueN(unplaced$match_id), " match(es): ", paste(head(unique(unplaced$match_id), 5), collapse = ", "))
  }

  # Scores must sit on rows in the order they happened. A misplaced score
  # breaks this even in a match the final-score check below has to skip
  # (one with unplaced scores), so it is checked on every match.
  placed <- se[!is.na(at_order)][order(match_id, event_number)]
  disorder <- placed[, .(bad = any(diff(at_order) < 0)), by = match_id][bad == TRUE]
  if (nrow(disorder) > 0) {
    stop("Season ", season, ": scores placed out of order in ", nrow(disorder), " match(es): ",
         paste(head(disorder$match_id, 5), collapse = ", "))
  }
  # One row can carry more than one score (rare); it keeps the last.
  placed <- placed[, .SD[.N], by = .(match_id, display_order = at_order)]
  ev[placed, on = .(match_id, display_order), `:=`(
    score_type = i.score_type, score_team_id = i.score_team_id, score_player_id = i.score_player_id,
    home_score = i.agg_home, away_score = i.agg_away)]
  ev[, `:=`(home_score = data.table::nafill(home_score, "locf"), away_score = data.table::nafill(away_score, "locf")), by = match_id]
  ev[is.na(home_score), `:=`(home_score = 0L, away_score = 0L)]

  # Every match whose scores all found a row must end on its official score.
  fin <- ev[, .(h = data.table::last(home_score), a = data.table::last(away_score),
                oh = data.table::last(home_team_score_total_score),
                oa = data.table::last(away_team_score_total_score)), by = match_id]
  fin <- fin[!match_id %in% unplaced$match_id & !is.na(oh)]
  off <- fin[h != oh | a != oa]
  cat("Running score ends on the official final score in", nrow(fin) - nrow(off), "of", nrow(fin), "matches\n")
  if (nrow(off) > 0) {
    stop("Season ", season, ": running score does not end on the final score in ", nrow(off), " match(es): ",
         paste(head(off$match_id, 5), collapse = ", "))
  }

  nm <- function(g, s) data.table::fifelse(is.na(g) & is.na(s), NA_character_,
                                           trimws(paste(data.table::fcoalesce(g, ""), data.table::fcoalesce(s, ""))))
  events <- ev[, .(
    match_id,
    season = as.integer(season),
    round_number,
    display_order,
    period = as.integer(period),
    period_seconds = as.integer(period_seconds),
    chain_number = as.integer(chain_number),
    repeat_of_prev,
    frame_team_id = chain_team_id,
    description,
    team_id,
    player_id,
    player_name = nm(player_name_given_name, player_name_surname),
    jumper_number = as.integer(jumper_number),
    player_position,
    lead_player_id,
    pos_team,
    disposal,
    shot_at_goal = !is.na(shot_at_goal) & shot_at_goal == TRUE,
    behind_info,
    x = as.integer(x),
    # The play-by-play's sign (see the y check): y > 0 is the top of the
    # ground with frame_team_id attacking to the right.
    y = -as.integer(y),
    initial_state,
    final_state,
    ep_team_id,
    exp_pts = round(exp_pts, 4),
    delta_ep = round(delta_ep, 4),
    wp = round(wp, 4),
    wpa = round(wpa, 4),
    wpa_disp = round(wpa_disp, 4),
    wpa_recv = round(wpa_recv, 4),
    player_credit = round(player_credit, 4),
    contest_target_id,
    contest_target_team_id,
    contest_defender_id,
    contest_defender_team_id,
    contest_outcome,
    score_type,
    score_team_id,
    score_player_id,
    home_score,
    away_score,
    home_final = as.integer(home_team_score_total_score),
    away_final = as.integer(away_team_score_total_score),
    home_team_id,
    away_team_id,
    home_team = home_team_team_name,
    away_team = away_team_team_name,
    home_team_abbr = home_team_team_abbr,
    away_team_abbr = away_team_team_abbr,
    venue_name,
    venue_length = as.integer(venue_length),
    venue_width = as.integer(venue_width)
  )]

  ev_file <- file.path("blog", paste0("chain-events-", season, ".parquet"))
  n_rg <- write_parquet_grouped(events, ev_file, by = c("round_number", "match_id", "display_order"))
  cat("Wrote", ev_file, ":", nrow(events), "rows (", nrow(pbp), "used by the EPV model ),",
      n_rg, "row groups (",
      round(file.info(ev_file)$size / 1024^2, 2), "MB )\n")
  }, error = function(e) {
    message("::error::chain-events-", season, " NOT written: ", conditionMessage(e))
  })
}

cat("\nDone.\n")
