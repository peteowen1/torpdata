# One row per possession chain, for the blog's AFL Chain Finder (torpdata#131).
# Built from the chain-events table build_afl_chains.R has just made, so the
# browser no longer aggregates ~448,000 event rows on every query.
#
# Every rule below copies the blog's own code, so the Chain Finder, the match
# chain viewer and the Turnovers tab give the same numbers:
#   inthegame-blog chain-viz.js aflModel()  -- chain team, points, net points,
#                                              expected-score change, win
#                                              probability swing, fast break
#   inthegame-blog scripts/build-afl-turnover-points.mjs  -- turnover blame and
#                                              points from a turnover
# (Rules confirmed by Pete 2026-10-13, and again 2026-11-10 on the 2026 Grand
# Final, in the torpdata#131 issue body.)
#
#   * Rows: the feed lists the event at every chain boundary twice; the copy
#     (repeat_of_prev) is dropped. Its np_value is folded into the row it
#     repeats, so chains still add up to the margin (chain-viz.js).
#     Exception (agreed with the blog session 2026-11-24): a copy that is the
#     whole of its own chain AND carries a score is kept as that chain's row,
#     with its own np_value (all its rows, if it has several; 2021-2026 has only
#     one-row ones). These are rushed-behind chains, 5-11 a
#     season; dropping them lost the point. Unscored copy-only chains (~250 a
#     season) are still dropped: keeping them would change which chain counts
#     as the "next chain" after a turnover, which is a rule change, not a fix.
#   * Chain team: the side that scored in it (the last score row's team; a
#     rushed behind belongs to the side it was rushed against, as the feed
#     records it), otherwise the side with most rows (ties: the side seen
#     first), otherwise the side the first row's value is shown for.
#   * points: the chain team's own score rows (goal 6, behind 1, rushed 1).
#   * net_points: the net points ledger's per-row values (np_value), from the
#     chain team's side. NA for a match with no ledger values.
#   * ep_change: the change in expected score (delta_ep) across the chain, from
#     the chain team's side (a row whose value belongs to the other side counts
#     against). The chain viewer's "Change".
#   * wp_start: the chain team's win probability at the chain's first row that
#     has one; wpa: the chain's win probability change for the chain team;
#     wp_end = wp_start + wpa.
#   * fast_break: the chain team's first row is 60 m or less from its own goal
#     (at least 50 m short of its forward 50) and one of its rows is inside its
#     forward 50 within 15 s of the chain's first row. fast_break_secs is that
#     time.
#   * Turnovers (build-afl-turnover-points.mjs): a chain whose last row ends in
#     a turnover is charged to its last kick or handball (the last row with an
#     AFL disposal tag and a player), and only when that disposal is tagged
#     clanger or ineffective (turnover_blame_player_id). An effective disposal
#     lost later in a contest is nobody's fault. A chain with a "Shot At Goal"
#     row from that disposal on is a missed shot, not a turnover: no blame and
#     no points_from_turnover.
#   * points_from_turnover: on the chain that LOST the ball, what the other side
#     scored in the very next chain, in the same quarter (0 if nothing). NA on
#     every chain that is not a counted turnover. turnover_won_by_player_id: the
#     first named player of the other side in that next chain.
#   * Positions: start_x/y and end_x/y are metres from the centre in the chain
#     team's frame (x > 0 = the end it attacks; drawn attacking right, y > 0 is
#     the top of the ground), from the first and last rows that have a spot.
#   * Clock: start_seconds/end_seconds are seconds into the period; duration is
#     end minus start. n_actions counts rows after dropping boundary copies.

suppressMessages(library(data.table))

CHAIN_SCORE_PTS <- c(GOAL = 6L, BEHIND = 1L, RUSHED_BEHIND = 1L)

# Sets repeat_of_prev to "this copy is dropped", by reference: NA becomes FALSE,
# and a scored copy that is the whole of its chain is kept (see the header).
# Shared by the build and its check so both count the same rows.
.mark_droppable_copies <- function(ev) {
  ev[is.na(repeat_of_prev), repeat_of_prev := FALSE]
  ev[, keep_copy := repeat_of_prev & all(repeat_of_prev) & any(!is.na(score_type)),
     by = .(match_id, chain_number)]
  ev[keep_copy == TRUE, repeat_of_prev := FALSE]
  ev[, keep_copy := NULL]
  invisible(ev)
}

#' @param events chain-events rows for one season (build_afl_chains.R `events`).
#' @return data.table, one row per (match_id, chain_number).
build_chain_summary <- function(events) {
  ev <- as.data.table(events)
  need <- c("match_id", "season", "round_number", "display_order", "period", "period_seconds",
            "chain_number", "repeat_of_prev", "frame_team_id", "description", "team_id",
            "player_id", "disposal", "x", "y", "initial_state", "final_state", "ep_team_id",
            "delta_ep", "np_value", "wp", "wpa", "score_type", "score_team_id",
            "home_team_id", "away_team_id", "home_team", "away_team", "venue_length")
  miss <- setdiff(need, names(ev))
  if (length(miss)) stop("chain-events lacks: ", paste(miss, collapse = ", "))
  ev <- ev[, ..need]
  setorder(ev, match_id, display_order)
  .mark_droppable_copies(ev)

  # Fold each boundary copy's np_value into the last kept row before it.
  ev[, kept_pos := cummax(fifelse(repeat_of_prev, 0L, seq_len(.N))), by = match_id]
  ev[, pos := seq_len(.N), by = match_id]
  fold <- ev[repeat_of_prev == TRUE & kept_pos > 0L & !is.na(np_value),
             .(fold = sum(np_value)), by = .(match_id, pos = kept_pos)]
  a <- ev[repeat_of_prev == FALSE]
  a <- fold[a, on = .(match_id, pos)]
  a[, np_row := fifelse(is.na(np_value) & is.na(fold), NA_real_,
                        fcoalesce(np_value, 0) + fcoalesce(fold, 0))]
  setorder(a, match_id, display_order)

  # Match clock: each period follows the last, as chain-viz.js lays them out.
  plen <- a[, .(len = max(fcoalesce(period_seconds, 0L))), by = .(match_id, period)]
  setorder(plen, match_id, period)
  plen[, t0 := cumsum(shift(len + 1L, fill = 0L)), by = match_id]
  a[plen, t := i.t0 + fcoalesce(period_seconds, 0L), on = .(match_id, period)]

  a[, `:=`(
    is_home_row = team_id == home_team_id,
    owner = fcoalesce(team_id, ep_team_id),
    pts = fcoalesce(unname(CHAIN_SCORE_PTS[score_type]), 0L)
  )]
  a[, sg := fifelse(!is.na(ep_team_id) & !is.na(owner) & owner != ep_team_id, -1, 1)]
  a[, home_wp := fifelse(is.na(wp) | is.na(team_id), NA_real_, fifelse(team_id == home_team_id, wp, 1 - wp))]
  a[, home_wpa := fifelse(is.na(wpa) | is.na(team_id), 0, fifelse(team_id == home_team_id, wpa, -wpa))]
  # Rounded per row to 2 decimals before summing, half away from zero, as
  # chain-viz.js finishChain() does so its header equals the visible column.
  a[, value := sg * fcoalesce(delta_ep, 0)]
  a[, value := sign(value) * round(abs(value) * 100 + 1e-9) / 100]
  # x in the home side's frame (chain-viz.js px).
  a[, px := fifelse(frame_team_id == home_team_id, as.numeric(x), -as.numeric(x))]
  a[, py := fifelse(frame_team_id == home_team_id, as.numeric(y), -as.numeric(y))]
  match_np <- a[, .(has_np = any(!is.na(np_row))), by = match_id]

  first_named_of <- function(pid, tid, side) {
    i <- which(!is.na(pid) & !is.na(tid) & tid == side)[1]
    if (is.na(i)) NA_character_ else pid[i]
  }
  cs <- a[, {
    hid <- home_team_id[1]; aid <- away_team_id[1]
    sc <- which(!is.na(score_type))
    tm <- if (length(sc)) score_team_id[sc[length(sc)]] else NA_character_
    if (is.na(tm)) {
      tt <- team_id[!is.na(team_id)]
      if (length(tt)) {
        u <- unique(tt); n <- tabulate(match(tt, u))
        tm <- u[which.max(n)]          # which.max takes the first of any tie
      } else {
        o <- owner[!is.na(owner)]
        tm <- if (length(o)) o[1] else NA_character_
      }
    }
    home <- !is.na(tm) && tm == hid
    dir <- if (home) 1 else -1
    own <- which(!is.na(team_id) & team_id == tm)
    ownx <- own[!is.na(px[own])]
    xt <- px * dir; yt <- py * dir
    halfL <- as.numeric(venue_length[1]) / 2
    fb_secs <- NA_real_
    if (!is.na(tm) && length(ownx) && isTRUE(halfL > 0) && xt[ownx[1]] + halfL <= 60) {
      j <- ownx[xt[ownx] >= halfL - 50][1]
      if (!is.na(j) && t[j] - t[1] <= 15) fb_secs <- as.numeric(t[j] - t[1])
    }
    sp <- which(!is.na(px))
    vt <- owner
    ep_sum <- sum(ifelse(is.na(vt) | is.na(tm), 0, ifelse(vt == tm, value, -value)))
    wb <- which(!is.na(home_wp))[1]
    wpa_team <- if (is.na(tm)) NA_real_ else dir * sum(home_wpa)
    wp0 <- if (is.na(wb) || is.na(tm)) NA_real_ else if (home) home_wp[wb] else 1 - home_wp[wb]
    # turnover (build-afl-turnover-points.mjs)
    last_final <- final_state[.N]
    gi <- if (identical(last_final, "turnover")) {
      k <- which(!is.na(disposal) & !is.na(player_id)); if (length(k)) k[length(k)] else NA_integer_
    } else NA_integer_
    shot <- !is.na(gi) && any(grepl("Shot At Goal", description[gi:.N], ignore.case = TRUE), na.rm = TRUE)
    counted <- !is.na(gi) && !shot
    .(round = round_number[1], period = period[1],
      first_order = display_order[1],
      team_id = tm,
      team = if (is.na(tm)) NA_character_ else if (home) home_team[1] else away_team[1],
      opponent = if (is.na(tm)) NA_character_ else if (home) away_team[1] else home_team[1],
      is_home = if (is.na(tm)) NA else home,
      start_seconds = period_seconds[1], end_seconds = period_seconds[.N],
      duration = t[.N] - t[1],
      n_actions = .N,
      start_x = if (length(sp)) xt[sp[1]] else NA_real_,
      start_y = if (length(sp)) yt[sp[1]] else NA_real_,
      end_x = if (length(sp)) xt[sp[length(sp)]] else NA_real_,
      end_y = if (length(sp)) yt[sp[length(sp)]] else NA_real_,
      initial_state = initial_state[1], final_state = last_final,
      points = if (is.na(tm)) 0L else as.integer(sum(pts[!is.na(score_team_id) & score_team_id == tm])),
      home_pts = as.integer(sum(pts[!is.na(score_team_id) & score_team_id == hid])),
      away_pts = as.integer(sum(pts[!is.na(score_team_id) & score_team_id == aid])),
      net_points = if (is.na(tm)) NA_real_ else dir * sum(np_row, na.rm = TRUE),
      ep_change = round(ep_sum, 5),
      wp_start = wp0, wp_end = wp0 + wpa_team, wpa = wpa_team,
      fast_break = !is.na(fb_secs), fast_break_secs = fb_secs,
      first_player_id = if (length(k1 <- own[!is.na(player_id[own])])) player_id[k1[1]] else NA_character_,
      last_player_id = if (length(k1)) player_id[k1[length(k1)]] else NA_character_,
      tov_counted = counted,
      tov_team_id = if (counted) team_id[gi] else NA_character_,
      tov_player_id = if (counted) player_id[gi] else NA_character_,
      tov_disposal = if (counted) disposal[gi] else NA_character_,
      first_home_named = first_named_of(player_id, team_id, hid),
      first_away_named = first_named_of(player_id, team_id, aid),
      home_team_id = hid, away_team_id = aid)
  }, by = .(match_id, chain_number)]

  # The next chain in match order, same quarter (the Turnovers tab's rule).
  setorder(cs, match_id, first_order)
  cs[, `:=`(nx_period = shift(period, type = "lead"), nx_home_pts = shift(home_pts, type = "lead"),
            nx_away_pts = shift(away_pts, type = "lead"),
            nx_first_home = shift(first_home_named, type = "lead"),
            nx_first_away = shift(first_away_named, type = "lead")), by = match_id]
  cs[, same_q := !is.na(nx_period) & nx_period == period]
  cs[, tov_by_home := tov_team_id == home_team_id]
  cs[, points_from_turnover := fifelse(!tov_counted, NA_integer_,
                                       fifelse(!same_q, 0L, fifelse(tov_by_home, nx_away_pts, nx_home_pts)))]
  cs[, turnover_won_by_player_id := fifelse(tov_counted & same_q,
                                            fifelse(tov_by_home, nx_first_away, nx_first_home), NA_character_)]
  cs[, turnover_blame_player_id := fifelse(tov_counted & tov_disposal %in% c("clanger", "ineffective"),
                                           tov_player_id, NA_character_)]
  cs[, turnover_blame_disposal := fifelse(is.na(turnover_blame_player_id), NA_character_, tov_disposal)]
  cs <- match_np[cs, on = "match_id"]
  cs[has_np == FALSE, net_points := NA_real_]

  out <- cs[, .(match_id, season = as.integer(substr(match_id, 5, 8)), round, chain_number,
                team_id, team, opponent, is_home, period,
                start_seconds, end_seconds, duration, n_actions,
                start_x, start_y, end_x, end_y, initial_state, final_state,
                points, points_from_turnover, net_points, ep_change,
                wp_start, wp_end, wpa, fast_break, fast_break_secs,
                first_player_id, last_player_id,
                turnover_blame_player_id, turnover_blame_disposal, turnover_won_by_player_id)]
  setorder(out, team, match_id, chain_number, na.last = TRUE)
  out[]
}

#' Assertions the build must pass before writing (stops on failure).
#' @param cs build_chain_summary() output; @param events the same chain-events.
check_chain_summary <- function(cs, events) {
  ev <- copy(as.data.table(events))
  all_scores <- ev[!is.na(score_type) & !is.na(score_team_id),
                   .(p = sum(CHAIN_SCORE_PTS[score_type])), by = .(match_id, team_id = score_team_id)]
  .mark_droppable_copies(ev)
  n_chains <- uniqueN(ev[repeat_of_prev %in% FALSE, .(match_id, chain_number)])
  if (nrow(cs) != n_chains) stop(sprintf("chain-summary: %d rows for %d chains", nrow(cs), n_chains))
  if (anyDuplicated(cs[, .(match_id, chain_number)])) stop("chain-summary: duplicate (match_id, chain_number)")
  tov_ev <- uniqueN(ev[repeat_of_prev %in% FALSE & final_state == "turnover", .(match_id, chain_number)])
  tov_cs <- sum(cs$final_state == "turnover", na.rm = TRUE)
  # chain-events' final_state is a chain-level field, so every row of a
  # turnover chain carries it; the summary reads it off the last row.
  if (tov_cs != tov_ev) stop(sprintf("chain-summary: %d turnover chains, chain-events has %d", tov_cs, tov_ev))
  if (mean(is.na(cs$team_id)) > 0.01) stop("chain-summary: more than 1% of chains have no team")
  # Points: per match and side, the chains' points equal ALL of chain-events'
  # score rows, dropped copies included. (Comparing against kept rows only hid
  # 5-11 lost rushed behinds a season.)
  pts_cs <- cs[!is.na(team_id), .(p = sum(points)), by = .(match_id, team_id)]
  j <- merge(all_scores, pts_cs, by = c("match_id", "team_id"), all = TRUE)
  j[is.na(p.x), p.x := 0]; j[is.na(p.y), p.y := 0]
  if (any(j$p.x != j$p.y)) stop("chain-summary: chain points differ from the score rows for ",
                                sum(j$p.x != j$p.y), " match-side(s), first ", j[p.x != p.y, match_id][1])
  # A turnover chain should not score for its own side. The feed itself does
  # this a handful of times (2024: 3, e.g. a behind then the opposition's
  # kick-in inside one chain), so it is logged, not fatal.
  n_tp <- cs[final_state == "turnover" & points > 0, .N]
  if (n_tp > 0) message("chain-summary: ", n_tp, " turnover chain(s) carry points, as the feed records them")
  invisible(list(chains = nrow(cs), turnovers = tov_cs))
}
