// Command query is the read side: a gRPC service over PostgreSQL.
//
// It is a separate process from the API on purpose. Reads and writes in this
// system have genuinely different shapes — writes are asynchronous through Kafka
// and must never block on the database, reads are synchronous and must be fast —
// so they scale, fail and get tuned independently.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gigagedianidze/order-pipeline/internal/config"
	orderv1 "github.com/gigagedianidze/order-pipeline/internal/gen/orderv1"
	"github.com/gigagedianidze/order-pipeline/internal/metrics"
	"github.com/gigagedianidze/order-pipeline/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	cfg := config.Load()
	log := cfg.Logger("query")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	addr := cfg.QueryListenAddr
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("listen", "addr", addr, "err", err)
		os.Exit(1)
	}

	go metrics.Serve(ctx, cfg.MetricsAddr, log)

	// One interceptor instruments every RPC, so a new method is measured the
	// moment it exists rather than whenever someone remembers to add a timer.
	srv := grpc.NewServer(grpc.UnaryInterceptor(instrument))
	orderv1.RegisterOrderServiceServer(srv, &server{db: db, log: log})
	// Reflection lets grpcurl and similar tools call the service without being
	// handed the .proto file. On an internal service that is a genuine
	// operability win; on a public one it would be an information leak.
	reflection.Register(srv)

	go func() {
		log.Info("listening", "addr", addr)
		if err := srv.Serve(lis); err != nil {
			log.Error("serve", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	// GracefulStop stops accepting connections and waits for in-flight RPCs.
	// Unlike the worker there are no offsets at stake, so the only requirement is
	// not to sever a response that is already being written.
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Warn("graceful stop timed out, forcing")
		srv.Stop()
	}
	log.Info("stopped")
}

// instrument records handling time and status code for every unary RPC.
func instrument(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {

	started := time.Now()
	res, err := handler(ctx, req)
	metrics.GRPCDuration.
		WithLabelValues(info.FullMethod, status.Code(err).String()).
		Observe(time.Since(started).Seconds())
	return res, err
}

type server struct {
	orderv1.UnimplementedOrderServiceServer
	db  *store.Store
	log *slog.Logger
}

func (s *server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	o, err := s.db.GetOrder(ctx, req.GetOrderId())
	if errors.Is(err, store.ErrNotFound) {
		// NotFound is expected traffic here, not a fault: the write path is
		// asynchronous, so a client reading straight after a 202 may arrive
		// before the worker has persisted the row.
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.GetOrderId())
	}
	if err != nil {
		// A malformed UUID reaches Postgres and comes back as 22P02. That is the
		// caller's mistake, so it must not be reported as an internal error.
		if !store.IsRetryable(err) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid order_id: %s", req.GetOrderId())
		}
		s.log.Error("get order", "order_id", req.GetOrderId(), "err", err)
		return nil, status.Error(codes.Internal, "could not read order")
	}
	return &orderv1.GetOrderResponse{Order: toProto(o)}, nil
}

func (s *server) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	orders, next, err := s.db.ListOrders(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetCustomerId())
	if err != nil {
		if !store.IsRetryable(err) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		s.log.Error("list orders", "err", err)
		return nil, status.Error(codes.Internal, "could not list orders")
	}

	out := make([]*orderv1.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, toProto(o))
	}
	return &orderv1.ListOrdersResponse{Orders: out, NextPageToken: next}, nil
}

func toProto(o store.Order) *orderv1.Order {
	items := make([]*orderv1.Item, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &orderv1.Item{
			Sku:            it.SKU,
			Quantity:       int32(it.Quantity),
			UnitPriceCents: it.UnitPriceCents,
		})
	}
	return &orderv1.Order{
		OrderId:     o.OrderID,
		EventId:     o.EventID,
		CustomerId:  o.CustomerID,
		Items:       items,
		TotalCents:  o.TotalCents,
		OccurredAt:  timestamppb.New(o.OccurredAt),
		ProcessedAt: timestamppb.New(o.ProcessedAt),
	}
}
