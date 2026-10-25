# Lock played finals into the season-simulation premiers odds (torpdata#108).
#
# torp's simulate_afl_season() keeps regular-season rounds only and re-simulates
# the whole finals bracket from the end-of-home-and-away ladder, so once finals
# are being played its premiers odds still describe a bracket that is already
# partly (or wholly) decided. Brisbane read 19.9% premiers after winning the flag.
#
# This file derives what the played finals fix (finish range per team) and then
# builds the per-stage finals probabilities from the simulations that agree with
# those results. It reads only torp's finals output, so torp is untouched.
#
# Exact once the Grand Final is played ("decided" mode). Part-way through the
# finals it is an approximation: a simulation is kept when every team's finish
# lands in its range, but the simulation output has only finals_finish per team,
# not who played whom. The bracket crosses over at the Preliminary Finals (QF1
# winner v SF2 winner), so after the Semis a kept simulation can have the right
# four teams in the wrong pairings. The fix needs the sim to return its fixtures;
# left to the browser/R sim replacement (inthegame-blog#805).
#
# finals_finish scale (torp simulate_finals, Final Ten System):
#   0 lost Wildcard, 1 lost Elimination Final, 2 lost Semi Final,
#   3 lost Preliminary Final, 4 runner-up, 5 premier.
#
# Bracket by finals round, in date order (AFL Annexure 1):
#   1 Wildcard (2 games)  2 Qualifying + Elimination Finals (4)  3 Semis (2)
#   4 Preliminary Finals (2)  5 Grand Final (1)

FINALS_ROUND_GAMES <- c(2L, 4L, 2L, 2L, 1L)
FINALS_TEAMS <- 10L
# Played finals shrink the usable simulations (each game keeps about 60-65%), so
# the build runs more of them once finals have started.
FINALS_SIMS_AFTER_START <- 20000L
FINALS_MIN_KEPT_SIMS <- 100L

# What the played finals fix for each team.
# `games`: one row per finals fixture of the season, played or not, with columns
#   round, home_team, away_team, actual_margin (home minus away; NA = unplayed).
# Returns list(constraints, n_played) where constraints has one row per team that
# has played a final: `lo` and `hi` bound its eventual finals_finish (lo == hi means
# its finish is known). Returns NULL when the fixtures do not look like the Final
# Ten bracket, so the caller can leave the odds unlocked instead of guessing.
finals_constraints <- function(games) {
  games <- as.data.frame(games)
  empty <- data.frame(team = character(), lo = integer(), hi = integer(),
                      stringsAsFactors = FALSE)
  if (nrow(games) == 0L) return(list(constraints = empty, n_played = 0L))

  rounds <- sort(unique(games$round))
  n_per  <- vapply(rounds, function(r) sum(games$round == r), integer(1))
  k <- length(rounds)
  if (k > length(FINALS_ROUND_GAMES) ||
      !all(n_per[-k] == FINALS_ROUND_GAMES[seq_len(k - 1L)]) ||
      n_per[k] > FINALS_ROUND_GAMES[k]) {
    message("::warning::Finals fixtures do not match the Final Ten bracket (games per round: ",
            paste(n_per, collapse = "/"), "); finals not locked")
    return(NULL)
  }

  wildcard_teams <- unique(c(games$home_team[games$round == rounds[1]],
                             games$away_team[games$round == rounds[1]]))
  rec <- list()
  add <- function(team, lo, hi) {
    rec[[length(rec) + 1L]] <<- data.frame(team = team, lo = lo, hi = hi,
                                           stringsAsFactors = FALSE)
  }
  n_played <- 0L
  for (i in seq_len(nrow(games))) {
    m <- games$actual_margin[i]
    if (is.na(m) || m == 0) next   # unplayed (finals cannot end level)
    n_played <- n_played + 1L
    stage <- match(games$round[i], rounds)
    h <- games$home_team[i]; a <- games$away_team[i]
    winner <- if (m > 0) h else a
    loser  <- if (m > 0) a else h
    if (stage == 1L) {                       # Wildcard: loser out, winner into week 1
      add(loser, 0L, 0L); add(winner, 1L, 5L)
    } else if (stage == 2L) {
      # An Elimination Final always has a Wildcard winner in it (5th/6th v the
      # reseeded 7th/8th); a Qualifying Final (1v4, 2v3) never does.
      if (h %in% wildcard_teams || a %in% wildcard_teams) {
        add(loser, 1L, 1L); add(winner, 2L, 5L)
      } else {                               # QF loser still plays a Semi Final
        add(loser, 2L, 5L); add(winner, 3L, 5L)
      }
    } else if (stage == 3L) {
      add(loser, 2L, 2L); add(winner, 3L, 5L)
    } else if (stage == 4L) {
      add(loser, 3L, 3L); add(winner, 4L, 5L)
    } else {
      add(loser, 4L, 4L); add(winner, 5L, 5L)
    }
  }
  if (length(rec) == 0L) return(list(constraints = empty, n_played = 0L))
  all <- do.call(rbind, rec)
  cons <- data.frame(team = sort(unique(all$team)), stringsAsFactors = FALSE)
  cons$lo <- as.integer(tapply(all$lo, all$team, max)[cons$team])
  cons$hi <- as.integer(tapply(all$hi, all$team, min)[cons$team])
  if (any(cons$lo > cons$hi)) {
    stop("finals results contradict each other for: ",
         paste(cons$team[cons$lo > cons$hi], collapse = ", "))
  }
  list(constraints = cons, n_played = n_played)
}

# TRUE when every one of the ten finalists' finish is known (the Grand Final is in).
finals_fully_decided <- function(cons) {
  nrow(cons) == FINALS_TEAMS && all(cons$lo == cons$hi)
}

# Number of simulations the build should run: more once finals are part-played,
# because the odds are then read from the simulations that match the results.
finals_sim_count <- function(fc, default = 3000L) {
  if (is.null(fc) || fc$n_played == 0L || finals_fully_decided(fc$constraints)) default
  else FINALS_SIMS_AFTER_START
}

.FINALS_STAGE_COLS <- c("lose_wildcard_pct", "lose_elim_pct", "lose_semi_pct",
                        "lose_prelim_pct", "runner_up_pct", "premiers_pct")  # finish 0..5

# Per-stage finals probabilities that agree with the played finals.
# `finals`: simulate_afl_season()$finals with team names already canonical
#   (columns sim_id, team, finals_finish). `cons`: finals_constraints()$constraints.
# Returns list(stage = data.table(team + the six stage columns), n_used, mode):
#   mode "decided" = Grand Final played, odds are the result, no simulation behind them;
#   mode "conditioned" = odds from the simulations whose finals match every played result;
#   "conditioned-alive-only" = same, relaxed to the teams still alive (see below).
lock_finals_probs <- function(finals, cons) {
  stage_row <- function(team, finish) {
    out <- as.list(stats::setNames(rep(0, 6L), .FINALS_STAGE_COLS))
    out[[.FINALS_STAGE_COLS[finish + 1L]]] <- 1
    data.table::as.data.table(c(list(team = team), out))
  }

  if (finals_fully_decided(cons)) {
    stage <- data.table::rbindlist(Map(stage_row, cons$team, cons$lo))
    return(list(stage = stage, n_used = 0L, mode = "decided"))
  }

  sims <- data.table::as.data.table(finals)[, .(sim_id, team, finals_finish)]
  missing_team <- setdiff(cons$team, unique(sims$team))
  if (length(missing_team) > 0L) {
    stop("finalist(s) never in the simulated top ten: ", paste(missing_team, collapse = ", "))
  }
  wide <- data.table::dcast(sims, sim_id ~ team, value.var = "finals_finish", fill = -1L)
  keep <- rep(TRUE, nrow(wide))
  for (j in seq_len(nrow(cons))) {
    f <- wide[[cons$team[j]]]
    keep <- keep & f >= cons$lo[j] & f <= cons$hi[j]
  }
  n_used <- sum(keep)
  mode <- "conditioned"
  if (n_used < FINALS_MIN_KEPT_SIMS) {
    # Matching all ten results leaves too few simulations once the Preliminary
    # Finals are in. When the teams still alive fill every remaining finish slot
    # exactly (two for the Grand Final, four after the Semis), they ARE the whole
    # remaining bracket, so requiring only that each reaches its stage is the same
    # condition and keeps far more simulations. Otherwise there is nothing sound
    # to relax, so refuse.
    alive <- cons[cons$lo < cons$hi, ]
    slots_from <- function(l) sum(c(2L, 2L, 2L, 2L, 1L, 1L)[(l + 1L):6L])
    relaxable <- nrow(cons) == FINALS_TEAMS && nrow(alive) > 0L &&
      min(alive$lo) >= 2L && nrow(alive) == slots_from(min(alive$lo))
    keep2 <- rep(TRUE, nrow(wide))
    if (relaxable) {
      for (j in seq_len(nrow(alive))) keep2 <- keep2 & wide[[alive$team[j]]] >= alive$lo[j]
    }
    if (!relaxable || sum(keep2) < FINALS_MIN_KEPT_SIMS) {
      stop("only ", n_used, " of ", nrow(wide), " simulations match the played finals (need ",
           FINALS_MIN_KEPT_SIMS, if (relaxable) paste0("; relaxed match leaves ", sum(keep2)) else "", ")")
    }
    keep <- keep2
    n_used <- sum(keep)
    mode <- "conditioned-alive-only"
  }
  kept <- sims[sim_id %in% wide$sim_id[keep]]
  stage <- kept[, {
    out <- lapply(0:5, function(k) sum(finals_finish == k) / n_used)
    names(out) <- .FINALS_STAGE_COLS
    out
  }, by = team]
  if (mode == "conditioned-alive-only") {
    # Teams already out take their real finish, not the one their simulation drew.
    done <- cons[cons$lo == cons$hi, ]
    stage <- data.table::rbindlist(c(
      list(stage[!team %in% done$team]),
      Map(stage_row, done$team, done$lo)), use.names = TRUE)
  }

  # Each stage has a fixed number of slots, so the columns must add up exactly.
  stopifnot(abs(sum(stage$premiers_pct) - 1) < 1e-9,
            abs(sum(stage$runner_up_pct) - 1) < 1e-9)
  list(stage = stage, n_used = as.integer(n_used), mode = mode)
}
