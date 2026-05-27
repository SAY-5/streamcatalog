#!/usr/bin/env bash
# Runs the lineage traversal benchmark and fails if it is more than the allowed
# percentage slower than the committed baseline. This is a smoke gate: it
# catches a large performance regression without being sensitive to normal
# run-to-run noise.
set -euo pipefail

ALLOWED_PCT="${1:-30}"
BASELINE_FILE="bench/baseline.txt"
BENCH="BenchmarkDownstreamTraversal"

baseline=$(awk -v b="$BENCH" '$1==b {print $2}' "$BASELINE_FILE")
if [ -z "$baseline" ]; then
  echo "no baseline for $BENCH in $BASELINE_FILE"
  exit 1
fi

out=$(go test -run='^$' -bench="^${BENCH}$" -benchmem -count=5 ./internal/lineage/)
echo "$out"

# Take the median ns/op across the five runs.
current=$(echo "$out" | awk -v b="$BENCH" '$1 ~ b {print $3}' | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
if [ -z "$current" ]; then
  echo "could not parse benchmark output"
  exit 1
fi

echo "baseline ns/op: $baseline"
echo "current  ns/op: $current (median of 5)"

awk -v base="$baseline" -v cur="$current" -v pct="$ALLOWED_PCT" 'BEGIN {
  limit = base * (1 + pct/100.0)
  printf "regression limit: %.0f ns/op (%d%% over baseline)\n", limit, pct
  if (cur+0 > limit) {
    printf "FAIL: %d ns/op exceeds limit %.0f ns/op\n", cur, limit
    exit 1
  }
  printf "OK: within %d%% smoke gate\n", pct
}'
