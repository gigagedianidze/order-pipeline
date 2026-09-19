#!/bin/bash
# Breaks the pipeline, then recovers it, and measures the recovery.
#
# chaos-db-outage.sh ends with records parked on the dead-letter topic: the
# experiment stops where most dead-letter queues do, with a count of what was
# lost to a break that lasted longer than the retry budget. Those records were
# never corrupt — the database was simply gone when their turn came — so the
# count is not a verdict, it is a backlog.
#
# This runs the other half. It induces the same outage, inspects the dead letters
# without touching them, replays them into the orders topic, and then asks
# PostgreSQL whether they are there. The claim being tested is not "the replay
# command ran" but "every order that fell out of the pipeline is now in the
# database", which is a row count, and the difference between a dead-letter queue
# and a recovery path.
set -uo pipefail

OUTAGE=${OUTAGE:-90}
RATE=${RATE:-1000}
DURATION=${DURATION:-150s}
TAG=${TAG:-replay-dlq}

mkdir -p results/reports

for cmd in loadgen replay; do
    if [ ! -x "./bin/${cmd}.exe" ]; then
        echo "building bin/${cmd}.exe"
        go build -o "bin/${cmd}.exe" "./cmd/${cmd}" || exit 1
    fi
done

echo "=== phase 1: break it ==="
echo "load: ${RATE}/s for ${DURATION}; postgres down for ${OUTAGE}s starting at t+20s"

./bin/loadgen.exe -rate "$RATE" -duration "$DURATION" -tag "$TAG" \
    -drain 600s -json "results/reports/${TAG}-load.json" >"results/reports/${TAG}-load.txt" 2>&1 &
LOAD_PID=$!

sleep 20
echo "$(date +%T) stopping postgres"
docker compose stop postgres >/dev/null 2>&1

sleep "$OUTAGE"
echo "$(date +%T) starting postgres"
docker compose start postgres >/dev/null 2>&1

wait $LOAD_PID
echo "$(date +%T) load finished"
grep -E "MISSING|count|accepted" "results/reports/${TAG}-load.txt" || true

# The worker needs its retry budget to run out before the dead letters exist at
# all, and the ones it wrote last are the ones whose absence would make the
# recovery look complete when it is not.
echo
echo "=== phase 2: look, without touching ==="
sleep 10
./bin/replay.exe -json "results/reports/${TAG}-dryrun.json" \
    2>&1 | tee "results/reports/${TAG}-dryrun.txt"

echo
echo "=== phase 3: replay, and verify the rows ==="
./bin/replay.exe -apply -verify 120s -json "results/reports/${TAG}-replay.json" \
    2>&1 | tee "results/reports/${TAG}-replay.txt"
STATUS=$?

echo
if [ $STATUS -eq 0 ]; then
    echo "recovered: every replayed record is in the database"
else
    echo "NOT recovered: see results/reports/${TAG}-replay.json"
fi
echo "reports: results/reports/${TAG}-{load,dryrun,replay}.{txt,json}"
exit $STATUS
