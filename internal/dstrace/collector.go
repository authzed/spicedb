// Package dstrace provides a context-scoped collector for attributing datastore
// query timings to a single Check dispatch node. It is populated by the
// observable datastore proxy and drained into the check debug trace, but lives
// in its own leaf package so neither side has to depend on the other.
package dstrace

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type collectorCtxKey struct{}

// Collector accumulates datastore query observations for a single Check
// dispatch node. It is safe for concurrent use: a node may issue multiple
// relationship queries in parallel (e.g. the branches of a union).
type Collector struct {
	mu      sync.Mutex
	queries []*v1.DatastoreQuery
}

// WithCollector returns a context carrying a fresh Collector, along with that
// collector. It should be called once per dispatch node when debug tracing is
// enabled so that the queries the node issues itself are attributed to it;
// dispatched children re-install their own collector and therefore capture their
// own queries.
func WithCollector(ctx context.Context) (context.Context, *Collector) {
	c := &Collector{}
	return context.WithValue(ctx, collectorCtxKey{}, c), c
}

// CollectorFromContext returns the Collector stored in ctx, or nil if none is
// present (i.e. debug tracing is disabled). A nil Collector is safe to call
// Record/Queries on.
func CollectorFromContext(ctx context.Context) *Collector {
	c, _ := ctx.Value(collectorCtxKey{}).(*Collector)
	return c
}

// Record appends a single datastore query observation. It is a no-op on a nil
// Collector.
func (c *Collector) Record(queryShape string, start time.Time, duration time.Duration, relationshipCount uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, &v1.DatastoreQuery{
		QueryShape:        queryShape,
		StartTime:         timestamppb.New(start),
		Duration:          durationpb.New(duration),
		RelationshipCount: relationshipCount,
	})
}

// Queries returns the collected datastore query observations. It returns nil on
// a nil Collector.
func (c *Collector) Queries() []*v1.DatastoreQuery {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queries
}
