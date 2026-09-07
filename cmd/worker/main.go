// Command worker is a placeholder until Day 3 — Kafka consumer group + idempotent persistence.
package main

import "orderpipeline/internal/config"

func main() {
	config.Load().Logger("worker").Info("not implemented yet", "planned", "Day 3 — Kafka consumer group + idempotent persistence")
}
