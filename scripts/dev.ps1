# PowerShell mirror of the Makefile, for Windows hosts without GNU make.
#   .\scripts\dev.ps1 up | down | reset | ps | logs | topics | smoke | build | test
param([Parameter(Position = 0)][string]$Target = "help")

$ErrorActionPreference = "Stop"
Push-Location (Join-Path $PSScriptRoot "..")
try {
    switch ($Target) {
        "up"     { docker compose up -d; docker compose logs kafka-init }
        "down"   { docker compose down }
        "reset"  { docker compose down -v }
        "ps"     { docker compose ps }
        "logs"   { docker compose logs -f }
        "topics" { docker exec edp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe }
        "smoke"  { go run ./cmd/smoke }
        "load"    { go run ./cmd/loadgen -rate 100 -duration 30s }
        "load-1k" { go run ./cmd/loadgen -rate 1000 -duration 30s }
        "build"  { go build ./... }
        "test"   { go test ./... }
        default  { "targets: up down reset ps logs topics smoke load load-1k build test" }
    }
}
finally { Pop-Location }
