// Package metrics defines the instrumentation every service exposes.
//
// The set is deliberately small. Four numbers answer the questions this system
// is actually asked — is it keeping up, how fast is it, is it failing, and is it
// giving up — and a metric nobody queries is a metric nobody maintains.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Outcome labels OrdersProcessed. Duplicates and dead-letters are *outcomes*,
// not errors: a duplicate rate that suddenly climbs means redelivery is
// happening, which is a real signal that a counter of "errors" would hide.
const (
	OutcomePersisted  = "persisted"
	OutcomeDuplicate  = "duplicate"
	OutcomeDeadLetter = "dead_letter"
)

var (
	// OrdersAccepted counts orders the API published to Kafka and answered 202.
	// The gap between this and OrdersProcessed{persisted} is the backlog, and it
	// should be flat over any window where the system is keeping up.
	OrdersAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_accepted_total",
		Help: "Orders accepted by the API and acknowledged by Kafka.",
	})

	OrdersRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_rejected_total",
		Help: "Orders rejected by the API, by reason.",
	}, []string{"reason"})

	OrdersProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_processed_total",
		Help: "Order events handled by a worker, by outcome.",
	}, []string{"status"})

	// ProcessingDuration is the worker's own write time: how long one record took
	// to persist, including retries.
	//
	// A histogram, not a summary or a gauge, because the question is "what is my
	// p99" and an average hides exactly the tail that matters. Buckets are chosen
	// around the measured ~15ms end-to-end so the interesting range has
	// resolution — default buckets would put almost everything in one bucket and
	// make the quantiles meaningless.
	ProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "order_processing_duration_seconds",
		Help:    "Time for a worker to persist one order event, including retries.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}, []string{"status"})

	// EndToEndLatency is occurred_at (API accepted) to persisted. This is the
	// number a user would actually feel, and it is only measurable because the
	// event carries its own creation time.
	EndToEndLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "order_end_to_end_latency_seconds",
		Help:    "Time from the API accepting an order to a worker persisting it.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})

	// ConsumerLag is the single most important number in the system: how many
	// records sit between the group's committed offset and the end of the log.
	//
	// Its *value* matters far less than its *derivative*. A steady lag of 5000 is
	// a system keeping up with a backlog; a lag of 500 climbing by 100/s is a
	// system falling behind and heading for trouble. Every other metric here can
	// look healthy while this one grows.
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_lag",
		Help: "Records between the consumer group's committed offset and the log end, per partition.",
	}, []string{"topic", "partition"})

	// PartitionsOwned makes the Day 3 observation queryable: with more workers
	// than partitions, some worker sits at zero forever.
	PartitionsOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_partitions_owned",
		Help: "Partitions currently assigned to this worker.",
	})

	// PartitionsPaused is the number of partitions this worker has stopped
	// fetching from because a record could be neither persisted nor parked.
	//
	// A gauge, not a counter, because the question is "is anything stuck right
	// now" — a cumulative count of pause events cannot answer that, and a
	// partition that pauses and recovers looks identical to one that never came
	// back. Anything above zero for more than a scrape or two is an alert.
	PartitionsPaused = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_partitions_paused",
		Help: "Partitions this worker has paused after failing to persist and to dead-letter.",
	})

	Retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "retries_total",
		Help: "Write attempts retried after a transient failure.",
	})

	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dlq_total",
		Help: "Records published to the dead-letter queue, by reason.",
	}, []string{"reason"})

	// ProduceDuration covers the API's synchronous produce, which sits directly
	// on the request path — so a slow broker shows up as slow HTTP.
	ProduceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_produce_duration_seconds",
		Help:    "Time for the API to produce one record and receive the broker ack.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	})

	// GRPCDuration instruments the read path, labelled so a slow method is
	// distinguishable from a slow service.
	GRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_server_handling_seconds",
		Help:    "Time for the query service to handle one RPC.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"method", "code"})
)

// Serve exposes /metrics on addr and blocks until ctx is cancelled.
//
// A separate port from the service's own traffic: metrics should stay scrapeable
// when the main listener is saturated, and should not be exposed wherever the
// service is.
func Serve(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("metrics listening", "addr", addr, "path", "/metrics")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server", "err", err)
	}
}
