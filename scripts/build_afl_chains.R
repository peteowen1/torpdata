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
  # torpdata#85: the chain-level possessing team, which defines the coordinate
  # frame for every (x, y) in this chain -- team_id is the ACTOR, not the
  # frame, and the two disagree on opponent-actor rows by construction
  # (clean_pbp.R step G). Falls back to team_id_mdl for pre-torp#92 seasons.
  "coord_team_id", "coord_home_team_id"
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
  "coord_team_id", "coord_home_team_id"
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
    coord_home_team_id
  )]
  data.table::setcolorder(out, OUTPUT_COLS)

  out_file <- file.path("blog", paste0("chains-", season, ".parquet"))
  arrow::write_parquet(as.data.frame(out), out_file)
  cat("Wrote", out_file, ":", nrow(out), "rows,", n_matches, "matches (",
      round(file.info(out_file)$size / 1024^2, 2), "MB )\n")
}

cat("\nDone.\n")
