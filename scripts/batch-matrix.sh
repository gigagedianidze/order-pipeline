#!/bin/bash
# Tests the claim Day 10 arrived at but did not measure.
#
# Day 10 concluded that partition count was never the binding constraint here:
# per-worker throughput *fell* as workers were added, a single worker barely
# moved when partitions went 3 -> 12, and 1828 ev/s implied about 0.55ms per
# write, which matched the measured p50 almost exactly. The worker processes
# records one at a time, so its ceiling is roughly 1/(database round trip).
#
# If that reasoning is right, amortising the round trip over a batch should move
# the ceiling, and nothing else about the system needs to change. If throughput
# stays flat as the batch grows, the bottleneck is somewhere else and the Day 10
# conclusion needs revisiting.
#
# One worker throughout, deliberately: this measures the per-worker write
# ceiling, and adding consumers would only confound it with coordination costs.
#
#   usage: scripts/batch-matrix.sh <batch sizes...>
#   e.g.   scripts/batch-matrix.sh 1 10 50 200
set -uo pipefail

BATCHES=("$@")
if [ ${#BATCHES[@]} -eq 0 ]; then
  echo "usage: batch-matrix.sh <batch sizes...>" >&2
  exit 1
fi

PARTITIONS=${PARTITIONS:-3}
RATE=${RATE:-5000}
DURATION=${DURATION:-10s}
PROM=${PROM:-http://localhost:9091}
GROUP=order-processors
OUT=${OUT:-results/batching.tsv}

k()     { MSYS_NO_PATHCONV=1 docker exec edp-kafka /opt/kafka/bin/"$@"; }
psql_() { MSYS_NO_PATHCONV=1 docker exec edp-postgres psql -U orders -d orders -t -A -c "$1"; }

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

  # See scaling-matrix.sh: the producer caches topic metadata and must be
  # restarted whenever the topic is recreated with fewer partitions.
  docker compose restart api >/dev/null 2>&1
  sleep 5
}

printf 'batch_size\tthroughput_evs\tp50_ms\tp95_ms\tp99_ms\tmax_ms\tpeak_lag\tpersisted\n' | tee "$OUT"

for B in "${BATCHES[@]}"; do
  TAG="batch-b${B}"

  docker compose up -d --scale worker=0 >/dev/null 2>&1
  sleep 3
  reset_topic

  # The knob under test. Compose reads it from the environment for the worker
  # service only, so nothing else about the run changes between configurations.
  WORKER_BATCH_SIZE="$B" docker compose up -d --scale worker=1 >/dev/null 2>&1
  sleep 20

  T0=$(date +%s)
  ./bin/loadgen.exe -rate "$RATE" -duration "$DURATION" -tag "$TAG" \
      -drain 600s -json "results/reports/${TAG}.json" >"results/reports/${TAG}.txt" 2>&1
  T1=$(date +%s)
  WINDOW=$((T1 - T0))

  PEAK_LAG=$(promq "max_over_time(sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\",topic=\"orders\"})[${WINDOW}s:5s])" "$T1")

  # Throughput from the rows themselves, for the reasons scaling-matrix.sh
  # explains at length: a Prometheus rate() window necessarily spans the
  # boundary between the load phase and the idle period after it.
  read -r COUNT SPAN <<<"$(psql_ "
    SELECT count(*),
           greatest(extract(epoch FROM (max(processed_at) - min(processed_at))), 0.001)
    FROM orders WHERE customer_id LIKE '${TAG}-%';" | tr '|' ' ')"
  THROUGHPUT=$(python -c "print(${COUNT:-0} / ${SPAN:-1})")

  # End-to-end latency is expected to get *worse* as the batch grows, because a
  # record waits for its whole batch. That trade is the finding, not a flaw:
  # throughput and per-record latency are being bought against each other, and a
  # table showing only one of them would be advocacy rather than measurement.
  read -r COUNT P50 P95 P99 MAXD <<<"$(psql_ "
    SELECT count(*),
           coalesce(percentile_cont(0.50) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY d),0)*1000,
           coalesce(max(d),0)*1000
    FROM (SELECT extract(epoch FROM (processed_at - occurred_at)) AS d
          FROM orders WHERE customer_id LIKE '${TAG}-%') t;" | tr '|' ' ')"

  printf '%s\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%s\n' \
    "$B" "$THROUGHPUT" "$P50" "$P95" "$P99" "$MAXD" "$PEAK_LAG" "$COUNT" | tee -a "$OUT"
done

# Leave the stack in the state every other experiment assumes.
docker compose up -d --scale worker=1 >/dev/null 2>&1

echo
echo "wrote $OUT"
