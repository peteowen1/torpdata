#!/usr/bin/env bash
# Upload every parquet in a folder to R2, several at a time.
#
#   r2_upload.sh <dir> <key-prefix>      e.g. r2_upload.sh blog inthegame-data/afl
#
# One at a time this took 90 s for 39 files (run 46111812122, 28 Nov 2026):
# about 2.3 s each, nearly all of it wrangler starting up, not bytes moving.
# R2_UPLOAD_JOBS sets how many run at once (default 6).
#
# Every file is attempted even when one fails, and the script exits 1 if any
# file failed after 3 attempts. Unchanged files are still re-uploaded: skipping
# them would save at most the ~15 s left and would stop refreshing each file's
# Last-Modified.

dir="${1:?usage: r2_upload.sh <dir> <key-prefix>}"
prefix="${2:?usage: r2_upload.sh <dir> <key-prefix>}"
jobs="${R2_UPLOAD_JOBS:-6}"
[[ "$jobs" =~ ^[1-9][0-9]?$ ]] || jobs=6

shopt -s nullglob
files=("$dir"/*.parquet)
if [ ${#files[@]} -eq 0 ]; then
  echo "::error::No parquet files found in $dir/ — R aggregation may have failed"
  exit 1
fi

upload_one() {
  local file="$1" key="$R2_PREFIX/$(basename "$1")" attempt=1 out
  while [ "$attempt" -le 3 ]; do
    # Cache-Control lets browsers/CF cache + revalidate via ETag (304)
    # instead of re-downloading megabytes (inthegame-blog #388).
    if out=$(wrangler r2 object put "$key" --file "$file" \
               --cache-control "public, max-age=300" --remote 2>&1); then
      echo "Uploaded $key"
      return 0
    fi
    if [ "$attempt" -lt 3 ]; then
      printf '%s
::warning::Upload attempt %s/3 failed for %s, retrying in %ss
'         "$out" "$attempt" "$key" "$(( 2 ** attempt ))"
      sleep $(( 2 ** attempt ))
    fi
    attempt=$(( attempt + 1 ))
  done
  # One printf so parallel uploads cannot interleave inside this block.
  printf '%s\n::error::Failed to upload %s after 3 attempts\n' "$out" "$key"
  return 1
}
export -f upload_one
export R2_PREFIX="$prefix"

if printf '%s\0' "${files[@]}" | xargs -0 -n 1 -P "$jobs" bash -c 'upload_one "$1"' _; then
  echo "Uploaded ${#files[@]} files to R2 ($prefix/, $jobs at a time)"
else
  echo "::error::At least one R2 upload failed; see the errors above"
  exit 1
fi
