GO ?= go

# The compose stack already serves this DSN; the integration tests skip without it.
POSTGRES_TEST_DSN ?= postgres://orders:orders@localhost:5433/orders?sslmode=disable

.PHONY: help up down logs ps topics smoke build test test-race test-integration test-all \
        load load-1k matrix ramp batch-matrix chaos-db chaos-kafka clean reset

help:          ## list the targets
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t14

up:            ## start Kafka + Postgres and create topics
	docker compose up -d
	docker compose logs kafka-init

down:          ## stop everything, keep data
	docker compose down

reset:         ## stop everything and delete Kafka/Postgres volumes
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

topics:        ## show topics and their partitions
	docker exec edp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe

smoke:         ## Day 1 acceptance: connect to Postgres and Kafka, exit 0
	$(GO) run ./cmd/smoke

load:          ## steady 100/s for 30s
	$(GO) run ./cmd/loadgen -rate 100 -duration 30s

load-1k:       ## steady 1000/s for 30s
	$(GO) run ./cmd/loadgen -rate 1000 -duration 30s

matrix:        ## scaling matrix: 3 partitions, 1/2/4/8 workers
	bash scripts/scaling-matrix.sh 3 1 2 4 8

ramp:          ## ramp offered load until something breaks
	bash scripts/ramp.sh 1000 2500 5000 10000 20000

batch-matrix:  ## does batching move the write ceiling? 1/10/50/200 records per round trip
	bash scripts/batch-matrix.sh 1 10 50 200

chaos-db:      ## remove PostgreSQL for 90s under load
	bash scripts/chaos-db-outage.sh

chaos-kafka:   ## remove the broker for 45s under load
	bash scripts/chaos-kafka.sh

build:
	$(GO) build ./...

test:          ## unit tests: no Docker, no network
	$(GO) test ./...

test-race:     ## unit tests under the race detector (needs cgo and a C toolchain)
	CGO_ENABLED=1 $(GO) test -race ./...

test-integration: ## store tests against the running Postgres (needs `make up`)
	POSTGRES_TEST_DSN="$(POSTGRES_TEST_DSN)" $(GO) test -count=1 ./internal/store/

test-all: test test-integration ## everything that can run locally

clean:
	rm -rf bin
