// Command query is a placeholder until Day 7 — gRPC read service over Postgres.
package main

import "orderpipeline/internal/config"

func main() {
	config.Load().Logger("query").Info("not implemented yet", "planned", "Day 7 — gRPC read service over Postgres")
}
