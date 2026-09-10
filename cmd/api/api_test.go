package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	orderv1 "github.com/gigagedianidze/order-pipeline/internal/gen/orderv1"
	"github.com/gigagedianidze/order-pipeline/internal/order"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakePublisher captures what the write path would have sent to Kafka.
type fakePublisher struct {
	published []order.Event
	keys      []string
	err       error
	pingErr   error
}

func (f *fakePublisher) Publish(_ context.Context, key string, value any) (int32, int64, error) {
	if f.err != nil {
		return 0, 0, f.err
	}
	evt, ok := value.(order.Event)
	if !ok {
		return 0, 0, errors.New("published something that is not an order event")
	}
	f.published = append(f.published, evt)
	f.keys = append(f.keys, key)
	return 0, int64(len(f.published) - 1), nil
}

func (f *fakePublisher) Ping(context.Context) error { return f.pingErr }

// fakeOrders stands in for the query service.
type fakeOrders struct {
	order *orderv1.Order
	list  *orderv1.ListOrdersResponse
	err   error

	gotPageSize   int32
	gotPageToken  string
	gotCustomerID string
}

func (f *fakeOrders) GetOrder(_ context.Context, in *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &orderv1.GetOrderResponse{Order: f.order}, nil
}

func (f *fakeOrders) ListOrders(_ context.Context, in *orderv1.ListOrdersRequest, _ ...grpc.CallOption) (*orderv1.ListOrdersResponse, error) {
	f.gotPageSize, f.gotPageToken, f.gotCustomerID = in.GetPageSize(), in.GetPageToken(), in.GetCustomerId()
	if f.err != nil {
		return nil, f.err
	}
	if f.list == nil {
		return &orderv1.ListOrdersResponse{}, nil
	}
	return f.list, nil
}

func newTestAPI(p publisher, o orderv1.OrderServiceClient) http.Handler {
	return routes(p, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func do(t *testing.T, h http.Handler, method, target, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, target, nil)
	} else {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	var decoded map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("response is not JSON: %v (body %q)", err, w.Body.String())
		}
	}
	return w, decoded
}

const validOrder = `{"customer_id":"cust-1","items":[{"sku":"A","quantity":2,"unit_price_cents":1999}]}`

func TestCreateOrderAccepts(t *testing.T) {
	p := &fakePublisher{}
	w, body := do(t, newTestAPI(p, &fakeOrders{}), "POST", "/orders", validOrder, nil)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	if len(p.published) != 1 {
		t.Fatalf("published %d events, want 1", len(p.published))
	}
	evt := p.published[0]
	if evt.TotalCents != 3998 {
		t.Errorf("total_cents = %d, want 3998", evt.TotalCents)
	}
	// The partition key must be the order id, or events for one order can land on
	// different partitions and lose their ordering.
	if p.keys[0] != evt.OrderID {
		t.Errorf("partition key = %q, want the order id %q", p.keys[0], evt.OrderID)
	}
	if body["order_id"] != evt.OrderID {
		t.Errorf("responded with order_id %v, want %q", body["order_id"], evt.OrderID)
	}
}

// A broker that did not acknowledge means the order does not exist. Answering
// 202 anyway would be the system quietly losing it.
func TestCreateOrderRefusesWhenBrokerIsDown(t *testing.T) {
	p := &fakePublisher{err: errors.New("no brokers")}
	w, _ := do(t, newTestAPI(p, &fakeOrders{}), "POST", "/orders", validOrder, nil)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestCreateOrderRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty body", `{}`},
		{"no customer", `{"items":[{"sku":"A","quantity":1,"unit_price_cents":1}]}`},
		{"no items", `{"customer_id":"c","items":[]}`},
		{"zero quantity", `{"customer_id":"c","items":[{"sku":"A","quantity":0,"unit_price_cents":1}]}`},
		{"negative price", `{"customer_id":"c","items":[{"sku":"A","quantity":1,"unit_price_cents":-5}]}`},
		{"unknown field", `{"customer_id":"c","items":[],"discount":true}`},
		{"not json", `{`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakePublisher{}
			w, _ := do(t, newTestAPI(p, &fakeOrders{}), "POST", "/orders", tc.body, nil)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
			if len(p.published) != 0 {
				t.Error("invalid input reached the topic")
			}
		})
	}
}

// The point of the Idempotency-Key: a client that times out and retries its POST
// must not create a second order. Same key, same ids, and the worker's existing
// event_id deduplication does the rest.
func TestIdempotencyKeyMakesRetriesSafe(t *testing.T) {
	p := &fakePublisher{}
	h := newTestAPI(p, &fakeOrders{})
	headers := map[string]string{"Idempotency-Key": "checkout-abc-123"}

	first, firstBody := do(t, h, "POST", "/orders", validOrder, headers)
	second, secondBody := do(t, h, "POST", "/orders", validOrder, headers)

	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("statuses = %d, %d, want 202 both", first.Code, second.Code)
	}
	if firstBody["order_id"] != secondBody["order_id"] {
		t.Errorf("order_id changed on retry: %v then %v", firstBody["order_id"], secondBody["order_id"])
	}
	if p.published[0].EventID != p.published[1].EventID {
		t.Error("event_id changed on retry, so the worker would treat it as a new order")
	}
}

func TestWithoutIdempotencyKeyRetriesAreDistinctOrders(t *testing.T) {
	p := &fakePublisher{}
	h := newTestAPI(p, &fakeOrders{})

	do(t, h, "POST", "/orders", validOrder, nil)
	do(t, h, "POST", "/orders", validOrder, nil)

	if p.published[0].OrderID == p.published[1].OrderID {
		t.Error("two unkeyed requests produced the same order_id")
	}
}

func TestIdempotencyKeyIsValidated(t *testing.T) {
	cases := map[string]string{
		"too long":         strings.Repeat("k", order.MaxIdempotencyKeyLen+1),
		"contains a space": "key with space",
		"control char":     "key\x00null",
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			p := &fakePublisher{}
			w, _ := do(t, newTestAPI(p, &fakeOrders{}), "POST", "/orders", validOrder,
				map[string]string{"Idempotency-Key": key})

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
			if len(p.published) != 0 {
				t.Error("a request with an invalid key reached the topic")
			}
		})
	}
}

// Status codes must survive the hop from gRPC to HTTP. A downstream NotFound
// reported as a 500 would make a caller's mistake look like a server fault.
func TestGRPCStatusCodesMapToHTTP(t *testing.T) {
	cases := []struct {
		code codes.Code
		want int
	}{
		{codes.NotFound, http.StatusNotFound},
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.Internal, http.StatusInternalServerError},
		{codes.Unknown, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			o := &fakeOrders{err: status.Error(tc.code, "downstream said so")}
			w, _ := do(t, newTestAPI(&fakePublisher{}, o), "GET", "/orders/abc", "", nil)

			if w.Code != tc.want {
				t.Errorf("status = %d, want %d", w.Code, tc.want)
			}
		})
	}
}

func TestGetOrderRendersEveryField(t *testing.T) {
	o := &fakeOrders{order: &orderv1.Order{
		OrderId:    "order-1",
		EventId:    "event-1",
		CustomerId: "cust-1",
		Items: []*orderv1.Item{
			{Sku: "A", Quantity: 2, UnitPriceCents: 1999},
		},
		TotalCents:  3998,
		OccurredAt:  timestamppb.Now(),
		ProcessedAt: timestamppb.Now(),
	}}
	w, body := do(t, newTestAPI(&fakePublisher{}, o), "GET", "/orders/order-1", "", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	for _, field := range []string{"order_id", "event_id", "customer_id", "items", "total_cents", "occurred_at", "processed_at"} {
		if _, ok := body[field]; !ok {
			t.Errorf("response is missing %q", field)
		}
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items = %v, want one item", body["items"])
	}
}

func TestListOrdersPassesParametersThrough(t *testing.T) {
	o := &fakeOrders{}
	do(t, newTestAPI(&fakePublisher{}, o), "GET",
		"/orders?page_size=25&page_token=abc&customer_id=cust-9", "", nil)

	if o.gotPageSize != 25 {
		t.Errorf("page_size = %d, want 25", o.gotPageSize)
	}
	if o.gotPageToken != "abc" {
		t.Errorf("page_token = %q, want %q", o.gotPageToken, "abc")
	}
	if o.gotCustomerID != "cust-9" {
		t.Errorf("customer_id = %q, want %q", o.gotCustomerID, "cust-9")
	}
}

// An oversized page_size used to be converted straight to int32, where a large
// enough value wraps to a negative number and is silently read as "use the
// default" — a wrong answer given confidently.
func TestListOrdersClampsPageSize(t *testing.T) {
	o := &fakeOrders{}
	do(t, newTestAPI(&fakePublisher{}, o), "GET", "/orders?page_size=99999999999", "", nil)

	if o.gotPageSize != maxPageSize {
		t.Errorf("page_size = %d, want it clamped to %d", o.gotPageSize, maxPageSize)
	}
}

func TestListOrdersRejectsUnusablePageSize(t *testing.T) {
	for _, raw := range []string{"abc", "-1"} {
		t.Run(raw, func(t *testing.T) {
			w, _ := do(t, newTestAPI(&fakePublisher{}, &fakeOrders{}), "GET", "/orders?page_size="+raw, "", nil)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestListOrdersEmptyResultIsAnEmptyArray(t *testing.T) {
	w, body := do(t, newTestAPI(&fakePublisher{}, &fakeOrders{}), "GET", "/orders", "", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	// A JSON null here would break every client that iterates the field.
	orders, ok := body["orders"].([]any)
	if !ok {
		t.Fatalf("orders = %v, want an array", body["orders"])
	}
	if len(orders) != 0 {
		t.Errorf("orders has %d entries, want 0", len(orders))
	}
}

// Liveness must not depend on Kafka: restarting a healthy API because the broker
// is down turns a partial outage into a total one.
func TestHealthzIsIndependentOfKafka(t *testing.T) {
	p := &fakePublisher{pingErr: errors.New("no brokers")}
	w, _ := do(t, newTestAPI(p, &fakeOrders{}), "GET", "/healthz", "", nil)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 even with Kafka down", w.Code)
	}
}

// Readiness must depend on Kafka: an API that cannot reach the broker cannot
// accept a write, and should be taken out of rotation rather than serve 503s.
func TestReadyzReflectsKafka(t *testing.T) {
	up := &fakePublisher{}
	if w, _ := do(t, newTestAPI(up, &fakeOrders{}), "GET", "/readyz", "", nil); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 with Kafka up", w.Code)
	}

	down := &fakePublisher{pingErr: errors.New("no brokers")}
	if w, _ := do(t, newTestAPI(down, &fakeOrders{}), "GET", "/readyz", "", nil); w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 with Kafka down", w.Code)
	}
}
