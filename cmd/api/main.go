// Command api is the HTTP ingress. It validates an order, assigns it an id,
// publishes it to Kafka and answers 202. It never touches the database on the
// write path — persistence is the worker's job, asynchronously.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orderpipeline/internal/broker"
	"orderpipeline/internal/config"
	"orderpipeline/internal/order"
)

const maxBodyBytes = 1 << 20 // 1 MiB

func main() {
	cfg := config.Load()
	log := cfg.Logger("api")

	producer, err := broker.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, log)
	if err != nil {
		log.Error("start producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           routes(producer, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Day 5 gives the worker a proper shutdown sequence; the API's version is
	// simple because it holds no offsets — drain in-flight requests, flush the
	// producer, exit.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("listening", "addr", cfg.APIAddr, "topic", cfg.KafkaTopic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("stopped")
}

func routes(producer *broker.Producer, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", createOrder(producer, log))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func createOrder(producer *broker.Producer, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

		var req order.CreateRequest
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "malformed request body: "+err.Error())
			return
		}
		if err := req.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		evt := order.NewEvent(req)

		// Partition key is the order id, so all events for one order land on one
		// partition and are processed in order by exactly one consumer.
		partition, offset, err := producer.Publish(r.Context(), evt.OrderID, evt)
		if err != nil {
			// The broker did not acknowledge, so the order does not exist as far
			// as this system is concerned. Say so, rather than accepting it.
			log.Error("publish failed", "order_id", evt.OrderID, "err", err)
			writeError(w, http.StatusServiceUnavailable, "could not accept order, please retry")
			return
		}

		log.Info("order accepted",
			"order_id", evt.OrderID,
			"event_id", evt.EventID,
			"partition", partition,
			"offset", offset,
			"total_cents", evt.TotalCents,
		)

		// 202, not 201: the order is durably queued, not yet persisted. A client
		// that immediately GETs it may legitimately get a 404 for a short window.
		writeJSON(w, http.StatusAccepted, map[string]any{
			"order_id": evt.OrderID,
			"status":   "accepted",
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
