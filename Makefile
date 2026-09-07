GO ?= go

.PHONY: up down logs ps topics smoke build test clean reset

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

build:
	$(GO) build ./...

test:
	$(GO) test ./...

clean:
	rm -rf bin
