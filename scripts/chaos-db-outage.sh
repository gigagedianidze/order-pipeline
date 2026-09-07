#!/bin/bash
# Takes PostgreSQL away for longer than the worker's retry budget allows.
#
# Day 6 showed a 31-second outage surviving intact, because the retry budget is
# 45 seconds. This asks the opposite question: what happens past the boundary?
# The prediction is dead letters with reason "retries_exhausted" — a break that
# is designed, bounded and visible, rather than data loss.
set -uo pipefail

OUTAGE=${OUTAGE:-90}
RATE=${RATE:-1000}
DURATION=${DURATION:-150s}
TAG=${TAG:-chaos-db}

echo "load: ${RATE}/s for ${DURATION}; postgres down for ${OUTAGE}s starting at t+20s"

./bin/loadgen.exe -rate "$RATE" -duration "$DURATION" -tag "$TAG" \
    -drain 600s -json "reports/${TAG}.json" >"reports/${TAG}.txt" 2>&1 &
LOAD_PID=$!

sleep 20
echo "$(date +%T) stopping postgres"
docker compose stop postgres >/dev/null 2>&1

sleep "$OUTAGE"
echo "$(date +%T) starting postgres"
docker compose start postgres >/dev/null 2>&1

wait $LOAD_PID
echo "$(date +%T) load finished"
