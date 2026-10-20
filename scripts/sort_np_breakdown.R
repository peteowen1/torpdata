# blog/np-breakdown-{season}.parquet: the same rows, sorted by player_id and
# written in row groups that never split a player (parquet_helpers.R
# write_parquet_by_player). torp's player_game-data release has the file as one
# row group (137,056 rows, 1.59 MB for 2026), and /afl/player reads one player's
# rows (median 215) from it, so every page load downloaded all of it. Only the
# order and layout change; the rows, columns and values do not.
#
# Runs after build-blog-data.yml downloads the file, and rewrites it in place.
# Usage: Rscript scripts/sort_np_breakdown.R blog/np-breakdown-2026.parquet

suppressPackageStartupMessages(library(arrow))
source("scripts/parquet_helpers.R")  # write_parquet_by_player()

path <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(path) || !file.exists(path)) stop("usage: Rscript scripts/sort_np_breakdown.R <file>; not found: ", path)
need <- c("match_id", "player_id", "category", "value")
d <- as.data.frame(read_parquet(path))
if (!all(need %in% names(d))) stop(path, " has no ", paste(setdiff(need, names(d)), collapse = ", "))

# The download and the rewrite, in one fixed order, for comparison
canon <- function(x) {
  x <- x[need]
  x <- x[do.call(order, c(unname(as.list(x)), method = "radix")), , drop = FALSE]
  rownames(x) <- NULL
  x
}

# Write beside the original and swap it in only once the check passes
tmp <- paste0(path, ".tmp")
fail <- function(msg) { unlink(tmp); stop(path, ": ", msg, call. = FALSE) }
tryCatch(write_parquet_by_player(d, tmp, by = c("player_id", "match_id", "category"), rows = 1500L),
         error = function(e) fail(conditionMessage(e)))
back <- as.data.frame(read_parquet(tmp))
if (!identical(canon(back), canon(d))) fail("rewrite changed the rows or values, left the original in place")
if (!file.rename(tmp, path)) fail("could not replace the original with the sorted copy")
