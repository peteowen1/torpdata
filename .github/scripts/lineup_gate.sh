#!/usr/bin/env bash
# Decides whether any AFL premiership match starts between LOOKBACK_H hours ago
# and LOOKAHEAD_D days ahead. Prints run=true|false and a reason. Anything it
# cannot establish prints run=true. NOW_EPOCH replays another moment (testing).
set -u
LOOKBACK_H=${LOOKBACK_H:-12}
LOOKAHEAD_D=${LOOKAHEAD_D:-9}
now=${NOW_EPOCH:-$(date -u +%s)}
year=$(date -u -d "@$now" +%Y)
api="https://aflapi.afl.com.au/afl/v2"
out() { echo "run=$1"; echo "reason=$2"; exit 0; }

seasons=$(curl -sf --max-time 30 "$api/competitions/1/compseasons?pageSize=100") || out true "season list unreadable"
total=0; hits=0
for y in "$year" "$((year + 1))"; do
  id=$(echo "$seasons" | jq -r --arg p "CD_S${y}014" '.compSeasons[] | select(.providerId == $p) | .id' 2>/dev/null) || out true "season list unparseable"
  [ -z "$id" ] && continue
  m=$(curl -sf --max-time 30 "$api/matches?compSeasonId=$id&pageSize=500") || out true "fixtures for $y unreadable"
  pages=$(echo "$m" | jq -r '.meta.pagination.numPages' 2>/dev/null) || out true "fixtures for $y unparseable"
  [ "$pages" = "1" ] || [ "$pages" = "0" ] || out true "fixtures for $y span $pages pages"
  counts=$(echo "$m" | jq -r --argjson now "$now" --argjson lb "$LOOKBACK_H" --argjson la "$LOOKAHEAD_D" '
    [.matches[] | (.utcStartTime | if endswith("+0000") then .[0:19] + "Z" else error("not UTC") end | fromdateiso8601)] as $t
    | "\($t | length) \([$t[] | select(. >= $now - $lb * 3600 and . <= $now + $la * 86400)] | length)"' 2>/dev/null) || out true "a start time in $y could not be read"
  total=$((total + ${counts% *})); hits=$((hits + ${counts#* }))
done
[ "$total" -gt 0 ] || out true "no fixtures found for $year or $((year + 1))"
[ "$hits" -gt 0 ] && out true "$hits match(es) start within ${LOOKBACK_H}h back / ${LOOKAHEAD_D}d ahead"
out false "none of $total fixtures starts within ${LOOKBACK_H}h back / ${LOOKAHEAD_D}d ahead"
