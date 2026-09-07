#!/bin/bash
# Ramps the offered rate until something breaks, and records what.
#
# "Breaks" is defined up front, because otherwise every result can be described
# as breaking or not breaking after the fact:
#
#   - the API rejects orders (503) or the client sees connection failures
#   - accepted orders fail to reach the database within the drain window
#   - accept latency degrades non-linearly rather than proportionally
#
# A growing backlog is NOT breaking. It is the queue doing its job, and
# distinguishing the two is most of the point.
#
#   usage: scripts/ramp.sh <rate> [rate...]
set -uo pipefail

RATES=("$@")
[ ${#RATES[@]} -gt 0 ] || { echo "usage: ramp.sh <rate> [rate...]"; exit 1; }

DURATION=${DURATION:-15s}
PROM=${PROM:-http://localhost:9091}
GROUP=order-processors
OUT=${OUT:-results/ramp.tsv}

psql_() { MSYS_NO_PATHCONV=1 docker exec edp-postgres psql -U orders -d orders -t -A -c "$1"; }
promq() {
  curl -sG --data-urlencode "query=$1" --data-urlencode "time=$2" "$PROM/api/v1/query" \
    | python -c 'import sys,json;d=json.load(sys.stdin)["data"]["result"];print(d[0]["value"][1] if d else 0)'
}

printf 'rate\tachieved\taccepted\trejected_503\tfailed\tsched_behind\taccept_p99_ms\te2e_p99_ms\tpersisted\tmissing\tpeak_lag\tdlq\tretries\n' | tee "$OUT"

for R in "${RATES[@]}"; do
  TAG="ramp-${R}"
  T0=$(date +%s)
  ./bin/loadgen.exe -rate "$R" -duration "$DURATION" -tag "$TAG" \
      -drain 600s -json "results/reports/${TAG}.json" >"results/reports/${TAG}.txt" 2>&1
  T1=$(date +%s)
  W=$((T1 - T0))

  PEAK_LAG=$(promq "max_over_time(sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\",topic=\"orders\"})[${W}s:5s])" "$T1")
  DLQ=$(promq "sum(increase(dlq_total[${W}s]))" "$T1")
  RETRIES=$(promq "sum(increase(retries_total[${W}s]))" "$T1")

  python - "$R" "results/reports/${TAG}.json" "$PEAK_LAG" "$DLQ" "$RETRIES" <<'PY' | tee -a "$OUT"
import json, sys
rate, path, lag, dlq, retries = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
r = json.load(open(path))
e2e = r.get("end_to_end_latency") or {}
# A 503 is a response, not a transport error, so it does not appear in "failed".
# It is also the literal definition of the system breaking, so it gets its own
# column: the first version of this table reported failed=0 while the API was
# rejecting three out of every four orders.
rejected = r["status_codes"].get("503", 0)

print("\t".join([
    rate,
    f"{r['achieved_rate']:.0f}",
    str(r["accepted"]),
    str(rejected),
    str(r["failed"]),
    str(r["scheduler_behind"]),
    f"{r['accept_latency']['p99_seconds']*1000:.0f}",
    f"{e2e.get('p99_seconds',0)*1000:.0f}",
    str(e2e.get("count", 0)),
    str(e2e.get("missing", 0)),
    f"{float(lag):.0f}",
    f"{float(dlq):.0f}",
    f"{float(retries):.0f}",
]))
PY
done

echo
echo "wrote $OUT"
