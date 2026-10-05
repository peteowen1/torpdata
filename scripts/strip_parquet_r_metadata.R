# Strip oversized R attribute metadata from blog parquets before upload.
#
# arrow's write_parquet() stores a data frame's R attributes in the file
# footer (key "r", and again inside "ARROW:schema"). A frame that is still
# grouped, or carries per-row attributes, puts every row index in there:
# on 2026-10-06 football/shots.parquet was 39.8 MB of which 37.6 MB was this
# footer (2.2 MB of shots), and match-shots.parquet 54.5 MB of which 37.6 MB.
# The blog reads these files in the browser, which downloads the whole footer
# before any data. Browsers never use R attributes, so dropping them changes
# nothing a page reads.
#
# Only rewrites a file whose footer is over LIMIT bytes, keeps its
# row-group size (pages filter by row group), and logs each one.
#
# Usage: Rscript scripts/strip_parquet_r_metadata.R [dir]   (default: blog)

suppressPackageStartupMessages(library(arrow))

LIMIT <- 1e6
dir <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(dir)) dir <- "blog"
files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Checking", length(files), "parquet file(s) in", dir, "for oversized R metadata\n")

# Footer length: the 4-byte little-endian integer before the closing "PAR1".
footer_bytes <- function(f) {
  con <- file(f, "rb")
  on.exit(close(con))
  seek(con, file.size(f) - 8)
  readBin(con, "integer", n = 1, size = 4, endian = "little")
}

for (f in files) {
  r_bytes <- footer_bytes(f)
  if (r_bytes <= LIMIT) next
  reader <- ParquetFileReader$create(f)
  rows_per_group <- reader$ReadRowGroup(0)$num_rows
  before <- file.size(f)
  tab <- reader$ReadTable()
  tab$metadata$r <- NULL
  tmp <- paste0(f, ".tmp")
  write_parquet(tab, tmp, chunk_size = rows_per_group)
  check <- read_parquet(tmp, as_data_frame = FALSE)
  if (check$num_rows != tab$num_rows || check$num_columns != tab$num_columns) {
    unlink(tmp)
    stop(basename(f), ": rewrite changed the shape, left the original in place")
  }
  n_rows <- as.integer(tab$num_rows)
  # Release the reader before replacing the file: on Windows it holds the
  # original open and the rename fails.
  rm(reader, tab, check)
  invisible(gc())
  if (!file.rename(tmp, f)) {
    unlink(tmp)
    stop(basename(f), ": could not replace the original with the stripped copy")
  }
  cat(sprintf("  %s: footer was %.1f MB, file %.1f MB -> %.1f MB (%d rows)\n",
              basename(f), r_bytes / 1e6, before / 1e6, file.size(f) / 1e6, n_rows))
}
