// Command api is the HTTP ingress and the public face of the system.
//
// Writes and reads take completely different routes from here. A write is
// validated, published to Kafka and answered 202 — the API never touches the
// database on that path. A read is a synchronous gRPC call to the query service.
// The asymmetry is the architecture, not an accident of implementation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/broker"
	"github.com/gigagedianidze/order-pipeline/internal/config"
	orderv1 "github.com/gigagedianidze/order-pipeline/internal/gen/orderv1"
	"github.com/gigagedianidze/order-pipeline/internal/metrics"
	"github.com/gigagedianidze/order-pipeline/internal/order"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB
	readTimeout  = 3 * time.Second
)

func main() {
	cfg := config.Load()
	log := cfg.Logger("api")

	producer, err := broker.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, log)
	if err != nil {
		log.Error("start producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Lazy connection: NewClient does not block on the query service being up,
	// so the API starts and serves writes even while the read path is down.
	// Coupling ingress availability to a downstream read service would be a
	// self-inflicted outage.
	conn, err := grpc.NewClient(cfg.QueryGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial query service", "addr", cfg.QueryGRPCAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	orders := orderv1.NewOrderServiceClient(conn)

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           routes(producer, orders, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go metrics.Serve(ctx, cfg.MetricsAddr, log)

	go func() {
		log.Info("listening", "addr", cfg.APIAddr, "topic", cfg.KafkaTopic, "query", cfg.QueryGRPCAddr)
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

func routes(producer *broker.Producer, orders orderv1.OrderServiceClient, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", createOrder(producer, log))
	mux.HandleFunc("GET /orders/{id}", getOrder(orders, log))
	mux.HandleFunc("GET /orders", listOrders(orders, log))
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
			metrics.OrdersRejected.WithLabelValues("malformed_body").Inc()
			writeError(w, http.StatusBadRequest, "malformed request body: "+err.Error())
			return
		}
		if err := req.Validate(); err != nil {
			metrics.OrdersRejected.WithLabelValues("validation").Inc()
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		evt := order.NewEvent(req)

		// Partition key is the order id, so all events for one order land on one
		// partition and are processed in order by exactly one consumer.
		started := time.Now()
		partition, offset, err := producer.Publish(r.Context(), evt.OrderID, evt)
		metrics.ProduceDuration.Observe(time.Since(started).Seconds())
		if err != nil {
			metrics.OrdersRejected.WithLabelValues("produce_failed").Inc()
			// The broker did not acknowledge, so the order does not exist as far
			// as this system is concerned. Say so, rather than accepting it.
			log.Error("publish failed", "order_id", evt.OrderID, "err", err)
			writeError(w, http.StatusServiceUnavailable, "could not accept order, please retry")
			return
		}

		metrics.OrdersAccepted.Inc()
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

func getOrder(orders orderv1.OrderServiceClient, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()

		res, err := orders.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: r.PathValue("id")})
		if err != nil {
			writeGRPCError(w, log, "get order", err)
			return
		}
		writeJSON(w, http.StatusOK, orderJSON(res.GetOrder()))
	}
}

func listOrders(orders orderv1.OrderServiceClient, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()

		pageSize, err := strconv.Atoi(r.URL.Query().Get("page_size"))
		if err != nil {
			pageSize = 0 // let the query service apply its default
		}

		res, err := orders.ListOrders(ctx, &orderv1.ListOrdersRequest{
			PageSize:   int32(pageSize),
			PageToken:  r.URL.Query().Get("page_token"),
			CustomerId: r.URL.Query().Get("customer_id"),
		})
		if err != nil {
			writeGRPCError(w, log, "list orders", err)
			return
		}

		out := make([]map[string]any, 0, len(res.GetOrders()))
		for _, o := range res.GetOrders() {
			out = append(out, orderJSON(o))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"orders":          out,
			"next_page_token": res.GetNextPageToken(),
		})
	}
}

// writeGRPCError translates gRPC status codes into HTTP ones. Leaking a
// downstream code (or a bare 500) would make a caller's mistake look like a
// server fault and hide real faults among them.
func writeGRPCError(w http.ResponseWriter, log *slog.Logger, op string, err error) {
	switch status.Code(err) {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, "order not found")
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, status.Convert(err).Message())
	case codes.DeadlineExceeded:
		log.Error(op, "err", err)
		writeError(w, http.StatusGatewayTimeout, "query service timed out")
	case codes.Unavailable:
		log.Error(op, "err", err)
		writeError(w, http.StatusServiceUnavailable, "query service unavailable")
	default:
		log.Error(op, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func orderJSON(o *orderv1.Order) map[string]any {
	items := make([]map[string]any, 0, len(o.GetItems()))
	for _, it := range o.GetItems() {
		items = append(items, map[string]any{
			"sku":              it.GetSku(),
			"quantity":         it.GetQuantity(),
			"unit_price_cents": it.GetUnitPriceCents(),
		})
	}
	return map[string]any{
		"order_id":     o.GetOrderId(),
		"event_id":     o.GetEventId(),
		"customer_id":  o.GetCustomerId(),
		"items":        items,
		"total_cents":  o.GetTotalCents(),
		"occurred_at":  o.GetOccurredAt().AsTime().Format(time.RFC3339Nano),
		"processed_at": o.GetProcessedAt().AsTime().Format(time.RFC3339Nano),
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
