# Shared by build_blog_data.R and build_afl_chains.R. Run the scripts from the
# repo root (as build-blog-data.yml does): both source this by relative path.

# Write a parquet as one row group per value of `by[1]` (rows sorted by `by`),
# so the blog's fetchParquet filter can skip every other group using each
# group's min/max statistics. A single-row-group file cannot be skipped into:
# /afl/match read all 344,365 rows of match-events-2026 (4.0 MB, ~9 s on the
# page) to use one round's 1,425. Grouped by round, that read is 0.54 MB
# (measured 2026-09-27, best of three); a whole-file read costs ~16% more bytes
# at the same parse speed. Used for match-events and chain-events: one season per
# file each, so round-first keeps them in time order. game-logs and shots span seasons,
# and blog code has assumed their time order (three first-row-wins bugs,
# 2026-09); grouping them by season+round kept that order but made game-logs
# 77% and shots 190% bigger for every whole-file reader. Keep SNAPPY: the blog loads plain hyparquet with no
# extra decompressors, so zstd or gzip would break every page that reads these.
write_parquet_grouped <- function(df, path, by) {
  df <- as.data.frame(df)
  # An empty season is valid (the old plain write handled it). Slicing 1:0
  # below would throw, and the caller's tryCatch wraps the whole season loop,
  # so one empty season would silently skip every later season's file.
  if (nrow(df) == 0L) {
    write_parquet(df, path, compression = "snappy")
    return(invisible(0L))
  }
  df <- df[do.call(order, c(unname(as.list(df[by])), na.last = TRUE)), , drop = FALSE]
  grp <- match(df[[by[1]]], unique(df[[by[1]]]))  # NA keys form their own group
  starts <- c(1L, which(diff(grp) != 0L) + 1L)
  ends <- c(starts[-1] - 1L, nrow(df))
  tbl <- arrow::arrow_table(df)
  sink <- arrow::FileOutputStream$create(path)
  props <- arrow::ParquetWriterProperties$create(names(df), compression = "snappy")
  writer <- arrow::ParquetFileWriter$create(tbl$schema, sink, properties = props)
  for (i in seq_along(starts)) {
    writer$WriteTable(tbl[starts[i]:ends[i], ], chunk_size = ends[i] - starts[i] + 1L)
  }
  writer$Close()
  sink$close()
  n_rg <- arrow::ParquetFileReader$create(path)$num_row_groups
  if (n_rg != length(starts)) {
    stop(sprintf("%s: wrote %d row groups, expected %d (one per %s)", path, n_rg, length(starts), by[1]))
  }
  invisible(n_rg)
}

# Per-round history of the season simulation (torpdata#106). simulations.parquet
# is overwritten every run, so this file keeps every round's rows: key =
# season + round + team, every column the simulation returns (premiers_pct,
# runner_up_pct, top_N_pct, the ladder-position spread...) plus `as_at`.
# CI has no persistent disk, so the previous history is read back from R2;
# the first run has no history there, so it is seeded from the previous
# simulations.parquet snapshot on R2 (which the same run is about to overwrite)
# and from any `seed_paths`. Re-publishing a round replaces that round's rows.
# `new`: this run's sim_output (all teams, one season + round).
update_sim_history <- function(new, as_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
                               r2_base = "https://pub-ee4bf5b599a047f9ac2b9facc1587008.r2.dev/afl",
                               seed_paths = character()) {
  key <- c("season", "round", "team")
  read_try <- function(p) tryCatch(as.data.frame(arrow::read_parquet(p)),
                                   error = function(e) { message("INFO: no ", p, ": ", conditionMessage(e)); NULL })
  new <- as.data.frame(new)
  new$as_at <- as_at
  hist <- read_try(file.path(r2_base, "simulations-history.parquet"))
  if (is.null(hist) || nrow(hist) == 0L) {
    message("INFO: no history on R2 - seeding from the previous simulations.parquet snapshot")
    hist <- NULL
  }
  seeds <- c(list(hist),
             if (is.null(hist)) list(read_try(file.path(r2_base, "simulations.parquet"))),
             lapply(seed_paths, read_try))
  seeds <- Filter(function(x) !is.null(x) && nrow(x) > 0L, seeds)
  seeds <- lapply(seeds, function(x) { if (!"as_at" %in% names(x)) x$as_at <- NA_character_; x })
  all <- dplyr::bind_rows(c(seeds, list(new)))   # later rows win below; new is last
  all$season <- as.integer(all$season); all$round <- as.integer(all$round)
  if ("n_sims" %in% names(all)) all$n_sims <- as.integer(all$n_sims)
  all <- all[!duplicated(all[key], fromLast = TRUE), , drop = FALSE]
  all <- all[do.call(order, unname(as.list(all[c("season", "round", "team")]))), , drop = FALSE]
  # Checks: key unique, this round present in full, required columns populated
  stopifnot(!anyDuplicated(all[key]))
  cur <- all[all$season == new$season[1] & all$round == new$round[1], ]
  stopifnot(nrow(cur) == nrow(new))
  for (col in c("premiers_pct", "runner_up_pct", "top_1_pct", "n_sims", "as_at"))
    if (!col %in% names(all) || all(is.na(cur[[col]]))) stop("history column empty for current round: ", col)
  rr <- as.data.frame(table(all$season, all$round)); rr <- rr[rr$Freq > 0, ]
  message("history: ", nrow(all), " rows x ", ncol(all), " cols; ", nrow(rr), " season-rounds; seasons ",
          paste(range(all$season), collapse = "-"), "; rounds ", paste(range(all$round), collapse = "-"),
          "; teams/round ", paste(sort(unique(rr$Freq)), collapse = ","))
  all
}
