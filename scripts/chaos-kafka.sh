#!/bin/bash
# Takes the broker away under load.
#
# This is the one component whose loss the architecture cannot absorb. The API
# produces synchronously and answers 202 only on a broker acknowledgement, so
# with no broker there is nothing honest to say except "no". The prediction is
# 503s for the duration and a clean recovery afterwards — a hard failure of
# availability with no loss of correctness.
set -uo pipefail

OUTAGE=${OUTAGE:-45}
RATE=${RATE:-500}
DURATION=${DURATION:-120s}
TAG=${TAG:-chaos-kafka}

echo "load: ${RATE}/s for ${DURATION}; kafka down for ${OUTAGE}s starting at t+20s"

./bin/loadgen.exe -rate "$RATE" -duration "$DURATION" -tag "$TAG" \
    -drain 300s -json "reports/${TAG}.json" >"reports/${TAG}.txt" 2>&1 &
LOAD_PID=$!

sleep 20
echo "$(date +%T) stopping kafka"
docker compose stop kafka >/dev/null 2>&1

sleep "$OUTAGE"
echo "$(date +%T) starting kafka"
docker compose start kafka >/dev/null 2>&1

wait $LOAD_PID
echo "$(date +%T) load finished"
