# blog/player-skills-latest.parquet: each player's latest round from
# blog/player-skills.parquet (one row per player per round, 234 columns,
# ~16.5 MB). /afl/player only shows a player's current skills and compares them
# with other players' current skills, so it reads this (~560 rows) instead of
# the whole season (blog page speed registry, 2026-10-06).
#
# Latest = highest (season, round) per player_id, chosen explicitly: the full
# file is in time order, and a reader taking the first row per player got the
# oldest (round 0) one, which is how the player page showed pre-season skills.
#
# Usage: Rscript scripts/build_skills_latest.R   (no-op if the input is absent)

suppressPackageStartupMessages(library(arrow))

src <- "blog/player-skills.parquet"
dst <- "blog/player-skills-latest.parquet"
if (!file.exists(src)) {
  message("::warning::", src, " not present; ", dst, " not written")
  quit(status = 0)
}
d <- as.data.frame(read_parquet(src))
for (k in c("player_id", "season", "round")) if (!k %in% names(d)) stop(src, " has no ", k, " column")
d <- d[order(d$player_id, -as.numeric(d$season), -as.numeric(d$round), method = "radix"), , drop = FALSE]
latest <- d[!duplicated(d$player_id), , drop = FALSE]
rownames(latest) <- NULL
n_players <- length(unique(d$player_id))
if (nrow(latest) != n_players) stop("expected one row per player: ", nrow(latest), " rows for ", n_players, " players")
write_parquet(latest, dst, compression = "snappy")
cat(sprintf("player-skills-latest: %d players (of %d rows); rounds %s; %.2f MB\n",
            nrow(latest), nrow(d), paste(range(latest$round), collapse = "-"), file.size(dst) / 1e6))
