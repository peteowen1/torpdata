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

# Write a parquet sorted by `by`, in row groups of `rows` rows, so the blog's
# fetchParquet filter can skip every group whose min/max statistics on by[1]
# exclude the values it wants (blog data-loader.js _rowGroupRanges). For files
# a page filters on one key but whose groups would be too small one-per-value:
# ratings.parquet sorted by player_id puts a player's <=170 rows in one or two
# of ~28 groups, where one row group made /afl/player download all 8.9 MB to
# keep 170 rows (blog page speed registry, 2026-10-06). Sorting is by byte
# order (method = "radix"), the order parquet statistics compare in, so a
# locale cannot interleave keys across groups.
write_parquet_sorted <- function(df, path, by, rows = 5000L) {
  df <- as.data.frame(df)
  if (nrow(df) > 0L) df <- df[do.call(order, c(unname(as.list(df[by])), method = "radix")), , drop = FALSE]
  rownames(df) <- NULL
  write_parquet(df, path, compression = "snappy", chunk_size = rows)
  invisible(arrow::ParquetFileReader$create(path)$num_row_groups)
}

# {game-logs,game-stats,shots}-by-player.parquet (torpdata#112): the same rows
# and columns as the original, sorted by player_id then `by[-1]`, packed into
# row groups of up to `rows` rows that never split a player (a player with more
# than `rows` rows gets a group of their own). /afl/player filters on player_id
# and so reads exactly one group, where the originals are laid out for
# /afl/match (game-logs by season/round/match) or not at all, and the page read
# all of game-logs (5.1 MB), game-stats (1.3 MB) and shots (0.9 MB) to keep
# one player's rows. The originals are not re-sorted: match pages depend on
# their order. Sorting is radix (byte order), as in write_parquet_sorted.
# Min/max statistics are written for player_id only: a browser downloads the
# whole footer before any row, the footer grows with row groups x columns, and
# no page filters these files on another column. That and `rows` were set by
# measuring footer + one group (what a player read costs) on the live files,
# 2026-10-07: game-logs 333 -> 269 KB (1,500 rows), game-stats 308 -> 178 KB
# and shots 129 -> 87 KB (3,000 rows); bigger groups cost more than they save.
# Returns the row-group count and logs rows per group and footer size.
write_parquet_by_player <- function(df, path, by = c("player_id", "season", "round"), rows = 1500L) {
  df <- as.data.frame(df)
  if (!"player_id" %in% names(df) || by[1] != "player_id") stop(path, ": by-player file needs player_id as the first sort key")
  if (anyNA(df$player_id)) stop(sprintf("%s: %d rows with NA player_id", path, sum(is.na(df$player_id))))
  by <- intersect(by, names(df))
  df <- df[do.call(order, c(unname(as.list(df[by])), method = "radix")), , drop = FALSE]
  rownames(df) <- NULL
  # Greedy packing of whole players: start a new group when adding the next
  # player would take the current one past `rows`.
  run <- rle(df$player_id)
  grp <- integer(length(run$lengths)); g <- 1L; filled <- 0L
  for (i in seq_along(run$lengths)) {
    if (filled > 0L && filled + run$lengths[i] > rows) { g <- g + 1L; filled <- 0L }
    grp[i] <- g; filled <- filled + run$lengths[i]
  }
  sizes <- as.integer(tapply(run$lengths, grp, sum))
  ends <- cumsum(sizes); starts <- c(1L, head(ends, -1L) + 1L)
  tbl <- arrow::arrow_table(df)
  # Drop R's attribute metadata (footer key "r"), as strip_parquet_r_metadata.R
  # does. game-stats carried 8.7 MB of it on the 2026-10-07 build; the strip
  # step then rewrote the file at a fixed group size, splitting 18 players
  # across two groups. Browsers never read it.
  tbl$metadata$r <- NULL
  sink <- arrow::FileOutputStream$create(path)
  props <- arrow::ParquetWriterProperties$create(names(df), compression = "snappy",
                                                 write_statistics = stats::setNames(names(df) == "player_id", names(df)))
  writer <- arrow::ParquetFileWriter$create(tbl$schema, sink, properties = props)
  for (i in seq_along(starts)) {
    writer$WriteTable(tbl[starts[i]:ends[i], ], chunk_size = sizes[i])
  }
  writer$Close()
  sink$close()
  rdr <- arrow::ParquetFileReader$create(path)
  n_rg <- rdr$num_row_groups
  if (n_rg != length(sizes)) stop(sprintf("%s: wrote %d row groups, expected %d", path, n_rg, length(sizes)))
  if (rdr$num_rows != nrow(df)) stop(sprintf("%s: %d rows written, expected %d", path, rdr$num_rows, nrow(df)))
  con <- file(path, "rb"); seek(con, file.size(path) - 8L)
  footer <- readBin(con, "integer", 1L, size = 4L, endian = "little"); close(con)
  cat(sprintf("%s: %d rows, %d players, %d row groups (rows per group: min %d, median %d, max %d; largest player %d rows), footer %d KB, %.2f MB\n",
              path, nrow(df), length(run$lengths), n_rg, min(sizes), as.integer(stats::median(sizes)), max(sizes),
              max(run$lengths), footer %/% 1024L, file.info(path)$size / 1024^2))
  invisible(n_rg)
}

# chain-events-{season}-by-team.parquet: one row group per club, holding every
# row of every match that club played (both teams' rows: the team page's pass
# network needs the opponent's to tell a combination from a turnover), with
# `for_team` = the club's name as chain-events spells it (home_team/away_team).
# Each match appears twice, so the file is about twice chain-events' size, but
# /afl/team and /afl/player read one club's group (~1/9 of the season) by
# filtering for_team, instead of all 447,945 rows (~25 s on 2026-10-06).
# Match pages keep reading chain-events-{season} (one row group per round).
write_chain_events_by_team <- function(events, path) {
  ev <- data.table::as.data.table(events)
  teams <- sort(unique(c(ev$home_team, ev$away_team)), method = "radix")
  parts <- lapply(teams, function(t) {
    x <- ev[home_team == t | away_team == t]
    x[, for_team := t]
    x
  })
  out <- data.table::rbindlist(parts)
  data.table::setcolorder(out, c("for_team", setdiff(names(out), "for_team")))
  n_rg <- write_parquet_grouped(out, path, by = c("for_team", "round_number", "match_id", "display_order"))
  if (n_rg != length(teams)) stop(sprintf("%s: %d row groups for %d clubs", path, n_rg, length(teams)))
  if (nrow(out) != 2L * nrow(ev)) stop(sprintf("%s: %d rows, expected 2 x %d (each match under both clubs)", path, nrow(out), nrow(ev)))
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
  # NULL only for a genuine 404 ("nothing there yet"). Every other failure
  # (timeout, 5xx, DNS, truncated file) is re-thrown: treating it as "no history"
  # would seed a one-round file and overwrite the real history on R2.
  read_try <- function(p) {
    tmp <- tempfile(fileext = ".parquet"); on.exit(unlink(tmp))
    warn <- character()
    rc <- tryCatch(
      withCallingHandlers(utils::download.file(p, tmp, mode = "wb", quiet = TRUE),
                          warning = function(w) { warn <<- c(warn, conditionMessage(w)); invokeRestart("muffleWarning") }),
      error = function(e) { warn <<- c(warn, conditionMessage(e)); 1L })
    if (!identical(as.integer(rc), 0L)) {
      if (any(grepl("404", warn, fixed = TRUE))) { message("INFO: 404 for ", p); return(NULL) }
      stop("could not read ", p, ": ", paste(warn, collapse = "; "))
    }
    as.data.frame(arrow::read_parquet(tmp))
  }
  read_seed <- function(p) if (file.exists(p)) as.data.frame(arrow::read_parquet(p)) else read_try(p)
  new <- as.data.frame(new)
  new$as_at <- as_at
  hist <- read_try(file.path(r2_base, "simulations-history.parquet"))
  if (is.null(hist) || nrow(hist) == 0L) {
    message("INFO: no history on R2 - seeding from the previous simulations.parquet snapshot")
    hist <- NULL
  }
  seeds <- c(list(hist),
             if (is.null(hist)) list(read_try(file.path(r2_base, "simulations.parquet"))),
             lapply(seed_paths, read_seed))
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
  # Never publish a smaller history than the one we read: every old round must survive
  if (!is.null(hist)) {
    kept <- paste(all$season, all$round); old <- unique(paste(hist$season, hist$round))
    if (nrow(all) < nrow(hist) || !all(old %in% kept)) stop("history would shrink: ", nrow(hist), " -> ", nrow(all), " rows")
  }
  rr <- as.data.frame(table(all$season, all$round)); rr <- rr[rr$Freq > 0, ]
  message("history: ", nrow(all), " rows x ", ncol(all), " cols; ", nrow(rr), " season-rounds; seasons ",
          paste(range(all$season), collapse = "-"), "; rounds ", paste(range(all$round), collapse = "-"),
          "; teams/round ", paste(sort(unique(rr$Freq)), collapse = ","))
  all
}
