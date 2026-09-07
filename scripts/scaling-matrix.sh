#!/bin/bash
# Runs the scaling experiment: for each worker count, deliberately overload the
# pipeline and measure how fast it drains.
#
# Throughput is measured while a backlog exists. That is the only condition under
# which workers run flat out, so the persisted rate equals capacity rather than
# whatever the load generator happened to offer. Driving at a rate the system can
# comfortably keep up with would measure the generator, not the system.
#
#   usage: scripts/scaling-matrix.sh <partitions> <worker counts...>
#   e.g.   scripts/scaling-matrix.sh 3 1 2 4 8
set -uo pipefail

PARTITIONS=${1:?usage: scaling-matrix.sh <partitions> <worker counts...>}
shift
WORKERS=("$@")

RATE=${RATE:-5000}
DURATION=${DURATION:-10s}
PROM=${PROM:-http://localhost:9091}
GROUP=order-processors
OUT=${OUT:-scaling-p${PARTITIONS}.tsv}

k()  { MSYS_NO_PATHCONV=1 docker exec edp-kafka /opt/kafka/bin/"$@"; }
psql_() { MSYS_NO_PATHCONV=1 docker exec edp-postgres psql -U orders -d orders -t -A -c "$1"; }

# promq <expr> <eval_time> -> single scalar, or 0
#
# The evaluation time is explicit, and callers window every expression to the run
# being measured. A bare max_over_time(expr[6m:]) looks back six minutes from
# *now*, and since each configuration takes about ninety seconds, that silently
# reports the peak of previous configurations — it produced three identical
# throughput figures before the mistake was spotted.
promq() {
  curl -sG --data-urlencode "query=$1" --data-urlencode "time=$2" "$PROM/api/v1/query" \
    | python -c 'import sys,json;d=json.load(sys.stdin)["data"]["result"];print(d[0]["value"][1] if d else 0)'
}

reset_topic() {
  k kafka-topics.sh --bootstrap-server kafka:19092 --delete --topic orders >/dev/null 2>&1
  sleep 4
  k kafka-topics.sh --bootstrap-server kafka:19092 --create --topic orders \
     --partitions "$PARTITIONS" --replication-factor 1 >/dev/null 2>&1
  psql_ "truncate table orders;" >/dev/null
}

printf 'partitions\tworkers\tthroughput_evs\tp50_ms\tp95_ms\tp99_ms\tmax_ms\tpeak_lag\tpersisted\n' | tee "$OUT"

for N in "${WORKERS[@]}"; do
  TAG="scale-p${PARTITIONS}-w${N}"

  # Scale to zero before touching the topic: deleting a topic out from under a
  # live consumer group produces errors that have nothing to do with the
  # experiment.
  docker compose up -d --scale worker=0 >/dev/null 2>&1
  sleep 3
  reset_topic
  docker compose up -d --scale worker="$N" >/dev/null 2>&1

  # Let every member join and the group settle before load starts, so the run
  # does not measure a rebalance.
  sleep 20

  T0=$(date +%s)
  ./bin/loadgen.exe -rate "$RATE" -duration "$DURATION" -tag "$TAG" \
      -drain 600s -json "reports/${TAG}.json" >"reports/${TAG}.txt" 2>&1
  T1=$(date +%s)
  WINDOW=$((T1 - T0))

  # Peak backlog, windowed to this run and evaluated at the moment it ended, so
  # no previous configuration can leak in.
  PEAK_LAG=$(promq "max_over_time(sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\",topic=\"orders\"})[${WINDOW}s:5s])" "$T1")

  # Throughput comes from the rows themselves: how many were persisted, divided
  # by the wall-clock span between the first and last of them.
  #
  # A Prometheus rate() cannot answer this cleanly here. Its window has to span
  # several scrapes to be meaningful, which means it also spans the boundary
  # between the 10s load phase and the idle period before it — smoothing away
  # exactly the burst being measured. The first attempt reported throughput as
  # flat across all worker counts while peak lag showed 4 workers holding the
  # backlog to a third of what 1 worker allowed, which cannot both be true.
  #
  # The table has no such window: it is the record of what actually happened.
  read -r COUNT SPAN <<<"$(psql_ "
    SELECT count(*),
           greatest(extract(epoch FROM (max(processed_at) - min(processed_at))), 0.001)
    FROM orders WHERE customer_id LIKE '${TAG}-%';" | tr '|' ' ')"
  THROUGHPUT=$(python -c "print(${COUNT:-0} / ${SPAN:-1})")

  read -r COUNT P50 P95 P99 MAXD <<<"$(psql_ "
    SELECT count(*),
           coalesce(percentile_cont(0.50) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(max(d),0)*1000
    FROM (SELECT extract(epoch FROM (processed_at - occurred_at)) AS d
          FROM orders WHERE customer_id LIKE '${TAG}-%') t;" | tr '|' ' ')"

  printf '%s\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%s\n' \
    "$PARTITIONS" "$N" "$THROUGHPUT" "$P50" "$P95" "$P99" "$MAXD" "$PEAK_LAG" "$COUNT" | tee -a "$OUT"
done

echo
echo "wrote $OUT"
