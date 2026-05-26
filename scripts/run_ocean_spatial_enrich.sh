#!/usr/bin/env bash
set -euo pipefail

current="${1:-2021-02-01}"
stop="${2:-2026-06-01}"
max_end="${3:-2026-05-24}"

while [ "$current" != "$stop" ]; do
  next=$(date -u -d "$current +1 month" +%Y-%m-01)
  end=$(date -u -d "$next -1 day" +%Y-%m-%d)
  if [[ "$end" > "$max_end" ]]; then
    end="$max_end"
  fi

  echo "$(date -Is) ocean_spatial_enrich $current $end"
  PGPASSWORD="@Maritime210526" psql \
    -h localhost \
    -p 5432 \
    -U "maritime-os" \
    -d "maritime-os" \
    -v ON_ERROR_STOP=1 \
    -v start="$current" \
    -v end="$end" \
    -f /home/rissets/indonesia-ocean-collector/scripts/ocean_spatial_enrich_fast.sql

  current="$next"
done
