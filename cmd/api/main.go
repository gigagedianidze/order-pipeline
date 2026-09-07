// Command api is a placeholder until Day 2 — HTTP ingress + Kafka producer + gRPC client.
package main

import "orderpipeline/internal/config"

func main() {
	config.Load().Logger("api").Info("not implemented yet", "planned", "Day 2 — HTTP ingress + Kafka producer + gRPC client")
}
