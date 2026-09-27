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
