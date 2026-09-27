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
  # (torp add_epv_vars(): exp_pts is in team_id_mdl's frame, torp#217)
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
  raw <- data.table::as.data.table(load_chains(season, rounds = TRUE))
  if (nrow(raw) == 0) stop("Season ", season, ": load_chains() returned no rows")
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
                   .pbp_x = x, .pbp_team = team_id, .pbp_desc = description)]
  ev <- merge(raw, model, by = c("match_id", "display_order"), all.x = TRUE, sort = FALSE)
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
  if (mean(fc_ok) < 0.999) {
    stop("Season ", season, ": frame check failed (", round(100 * mean(fc_ok), 2), "% agree)")
  }

  # Running score after each row. Goals and behinds have their own rows, but
  # a rushed behind usually does not (Grand Final 2026: 24 Behind rows for 29
  # behinds scored). Every behind is followed by a kick-in, though, so a
  # kick-in whose previous row is not a Behind is counted as a rushed behind
  # to the side NOT kicking in. That reproduces the API's final score in 209
  # of 218 matches in 2026 (4 of 218 counting scoring rows alone); the other
  # 9 are each exactly one behind short -- a rushed behind at a siren, which
  # has no kick-in after it and so leaves no trace in the feed. The official
  # final score is carried as home_final / away_final for headlines.
  data.table::setorder(ev, match_id, display_order)
  ev[, .prev := data.table::shift(description), by = match_id]
  ev[, .rushed := grepl("^Kickin", description) & (is.na(.prev) | .prev != "Behind")]
  ev[, .scorer := data.table::fcase(
    description %in% c("Goal", "Behind"), team_id,
    .rushed, data.table::fifelse(team_id == home_team_id, away_team_id, home_team_id),
    default = NA_character_)]
  ev[, .pts := data.table::fcase(description == "Goal", 6L,
                                 description == "Behind" | .rushed, 1L, default = 0L)]
  ev[, `:=`(home_score = cumsum(data.table::fifelse(!is.na(.scorer) & .scorer == home_team_id, .pts, 0L)),
            away_score = cumsum(data.table::fifelse(!is.na(.scorer) & .scorer == away_team_id, .pts, 0L))),
     by = match_id]
  fin <- ev[, .(h = data.table::last(home_score), a = data.table::last(away_score),
                oh = data.table::last(home_team_score_total_score),
                oa = data.table::last(away_team_score_total_score)), by = match_id]
  off <- fin[!is.na(oh) & (h != oh | a != oa)]
  cat("Running score matches the API final score in", nrow(fin) - nrow(off), "of", nrow(fin), "matches\n")
  if (nrow(off) > 0) {
    message("::warning::chain-events-", season, ": running score differs from the final score in ",
            nrow(off), " match(es) (expected: a few, from siren rushed behinds): ",
            paste(head(off$match_id, 5), collapse = ", "))
  }
  # Far more than the siren cases means the scoring rule itself has broken.
  if (nrow(fin) >= 20 && nrow(off) / nrow(fin) > 0.15) {
    stop("Season ", season, ": running score is wrong in ", nrow(off), " of ", nrow(fin), " matches")
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
    y = as.integer(y),
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
}

cat("\nDone.\n")
