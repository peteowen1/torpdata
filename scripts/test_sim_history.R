# Test for update_sim_history() (torpdata#133): a build that carries played
# finals results must not replace the round's last real simulation.
# Run from the repo root:  Rscript scripts/test_sim_history.R
# Needs arrow and dplyr. Reads "R2" from a local file:// directory, no network.

suppressPackageStartupMessages({ library(arrow); library(dplyr) })
source("scripts/parquet_helpers.R")

teams <- paste("Team", LETTERS[1:18])
mk <- function(round, premiers, locked, season = 2027L) {
  data.frame(team = teams, season = season, round = as.integer(round),
             premiers_pct = premiers, runner_up_pct = 0.1, top_1_pct = 0.1,
             n_sims = 3000L, finals_games_locked = as.integer(locked),
             finals_sims_used = if (locked == 0L) 3000L else 0L,
             stringsAsFactors = FALSE)
}

# Build the history as the workflow does: read the previous file from "R2"
# (a local dir here), add this run, write it back.
dir <- tempfile("r2-"); dir.create(dir)
r2 <- paste0("file://", dir)
run <- function(new, as_at) {
  h <- update_sim_history(new, as_at = as_at, r2_base = r2)
  write_parquet(h, file.path(dir, "simulations-history.parquet"))
  h
}
# First run: an empty history (a missing file would need an http 404), seeded from
# a simulations.parquet snapshot
write_parquet(mk(23, 0.05, 0L)[0, ], file.path(dir, "simulations-history.parquet"))
write_parquet(mk(23, 0.05, 0L), file.path(dir, "simulations.parquet"))

h1 <- run(mk(24, 0.14, 0L), "2027-09-01T00:00:00Z")          # last pre-finals simulation
stopifnot(nrow(h1) == 36, setequal(h1$round, c(23L, 24L)))

h2 <- run(mk(24, 1.0, 11L), "2027-09-27T05:00:00Z")          # same round, finals locked
r24 <- h2[h2$round == 24L, ]
stopifnot(
  nrow(h2) == nrow(h1),
  nrow(r24) == 18,
  all(r24$premiers_pct == 0.14),                             # the simulation survived
  all(r24$finals_games_locked == 0L),
  all(r24$as_at == "2027-09-01T00:00:00Z")
)

h3 <- run(mk(25, 1.0, 11L), "2027-09-28T05:00:00Z")          # a later, finals-only round
stopifnot(nrow(h3) == nrow(h1), !any(h3$finals_games_locked > 0L))

# A fresh unlocked rebuild of the same round still replaces it (latest sim wins)
h4 <- run(mk(24, 0.20, 0L), "2027-09-02T00:00:00Z")
stopifnot(all(h4$premiers_pct[h4$round == 24L] == 0.20), nrow(h4) == nrow(h1))

# A null finals_games_locked (rows from before the column existed) counts as a simulation
old <- mk(24, 0.30, 0L); old$finals_games_locked <- NA_integer_
h5 <- run(old, "2027-09-03T00:00:00Z")
stopifnot(all(h5$premiers_pct[h5$round == 24L] == 0.30))

cat("test_sim_history: all checks passed\n")
