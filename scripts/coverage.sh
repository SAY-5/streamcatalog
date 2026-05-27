#!/usr/bin/env bash
# Measures unit-test coverage of the business-logic packages and enforces a
# minimum threshold. The Postgres store and Kafka admin are covered by the
# integration suite, not the unit suite, so their files are excluded from this
# gate to keep the number honest.
set -euo pipefail

THRESHOLD="${1:-75}"
PKGS="github.com/SAY-5/streamcatalog/internal/api,github.com/SAY-5/streamcatalog/internal/lineage,github.com/SAY-5/streamcatalog/internal/schema,github.com/SAY-5/streamcatalog/internal/catalog"

go test -count=1 -coverpkg="$PKGS" -coverprofile=cover.out \
  ./internal/api/ ./internal/lineage/ ./internal/schema/ ./internal/catalog/

grep -vE 'internal/catalog/store.go|internal/kafkax/admin.go' cover.out > cover.filtered.out

total=$(go tool cover -func=cover.filtered.out | awk '/^total:/ {print $3}' | tr -d '%')
echo "unit coverage: ${total}% (threshold ${THRESHOLD}%)"

awk -v t="$THRESHOLD" -v c="$total" 'BEGIN { exit (c+0 < t+0) ? 1 : 0 }' || {
  echo "coverage ${total}% is below threshold ${THRESHOLD}%"
  exit 1
}
