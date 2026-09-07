package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerOptions configures a consumer-group member.
type ConsumerOptions struct {
	Brokers []string
	Topic   string
	GroupID string
	Log     *slog.Logger

	// ManualCommit disables franz-go's periodic auto-commit. Day 4 turns this on
	// so offsets advance only after a successful database write.
	ManualCommit bool
}

// NewConsumerGroup builds a client that joins GroupID and consumes Topic.
//
// The rebalance callbacks are not decoration: partition assignment is the single
// most useful thing to watch in this system. franz-go's default balancer is
// cooperative-sticky, so the callbacks report *deltas* — a member that keeps
// partitions 0 and 1 while handing 2 to a newcomer only sees "revoked: [2]", and
// empty callbacks fire routinely as rebalance rounds settle. Deltas alone are
// unreadable, so an Assignment tracks the running total and every log line says
// what this member owns right now.
func NewConsumerGroup(opts ConsumerOptions) (*kgo.Client, *Assignment, error) {
	log := opts.Log
	owned := &Assignment{}

	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topic),
		// A new group starts at the beginning of the topic rather than skipping
		// whatever was produced before it existed.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			if len(assigned) == 0 {
				return // a rebalance round that changed nothing for this member
			}
			owned.add(assigned)
			log.Info("partitions assigned", "gained", format(assigned), "now_owns", owned.String())
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			if len(revoked) == 0 {
				return
			}
			owned.remove(revoked)
			log.Info("partitions revoked", "gave_up", format(revoked), "now_owns", owned.String())
		}),
		// "Lost" is not "revoked": the member did not hand these back, the group
		// took them away — session timeout, or a poll loop that stalled too long.
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			if len(lost) == 0 {
				return
			}
			owned.remove(lost)
			log.Warn("partitions lost", "lost", format(lost), "now_owns", owned.String())
		}),
	}
	if opts.ManualCommit {
		kopts = append(kopts, kgo.DisableAutoCommit())
	}

	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return client, owned, nil
}

// Assignment is the set of topic-partitions this member currently owns. Rebalance
// callbacks run on the client's own goroutine while the poll loop reads it, so it
// is mutex-guarded.
type Assignment struct {
	mu    sync.Mutex
	owned map[string]map[int32]bool
}

func (a *Assignment) add(m map[string][]int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.owned == nil {
		a.owned = make(map[string]map[int32]bool)
	}
	for topic, parts := range m {
		if a.owned[topic] == nil {
			a.owned[topic] = make(map[int32]bool)
		}
		for _, p := range parts {
			a.owned[topic][p] = true
		}
	}
}

func (a *Assignment) remove(m map[string][]int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for topic, parts := range m {
		for _, p := range parts {
			delete(a.owned[topic], p)
		}
		if len(a.owned[topic]) == 0 {
			delete(a.owned, topic)
		}
	}
}

// Count is the number of partitions owned — zero means this member is idle,
// which is what happens when the group has more members than partitions.
func (a *Assignment) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, parts := range a.owned {
		n += len(parts)
	}
	return n
}

func (a *Assignment) String() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	m := make(map[string][]int32, len(a.owned))
	for topic, parts := range a.owned {
		for p := range parts {
			m[topic] = append(m[topic], p)
		}
	}
	return format(m)
}

// format renders an assignment map as "orders:[0 1 2]" with stable ordering, so
// log lines from two workers can be compared by eye.
func format(m map[string][]int32) string {
	topics := make([]string, 0, len(m))
	for t := range m {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	out := ""
	for _, t := range topics {
		parts := append([]int32(nil), m[t]...)
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		if out != "" {
			out += " "
		}
		out += fmt.Sprintf("%s:%v", t, parts)
	}
	if out == "" {
		return "none"
	}
	return out
}
