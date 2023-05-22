package pool

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lthibault/jitterbug"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	log "github.com/authzed/spicedb/internal/logging"
)

const errorBurst = 2

var healthyCRDBNodeCountGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "crdb_healthy_nodes",
	Help: "the number of healthy crdb nodes detected by spicedb",
})

func init() {
	prometheus.MustRegister(healthyCRDBNodeCountGauge)
}

// NodeHealthTracker detects changes in the node pool by polling the cluster periodically and recording
// the node ids that are seen. This is used to detect new nodes that come online that have either previously
// been marked unhealthy due to connection errors or due to scale up.
//
// Consumers can manually mark a node healthy or unhealthy as well.
type NodeHealthTracker struct {
	sync.RWMutex
	connConfig    *pgx.ConnConfig
	healthyNodes  map[uint32]struct{}
	nodesEverSeen map[uint32]*rate.Limiter
	newLimiter    func() *rate.Limiter
}

// NewNodeHealthChecker builds a health checker that polls the cluster at the given url.
func NewNodeHealthChecker(url string) (*NodeHealthTracker, error) {
	connConfig, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	return &NodeHealthTracker{
		connConfig:    connConfig,
		healthyNodes:  make(map[uint32]struct{}, 0),
		nodesEverSeen: make(map[uint32]*rate.Limiter, 0),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), errorBurst)
		},
	}, nil
}

// Poll starts polling the cluster and recording the node IDs that it sees.
func (t *NodeHealthTracker) Poll(ctx context.Context, interval time.Duration) {
	ticker := jitterbug.New(interval, jitterbug.Uniform{
		Source: rand.New(rand.NewSource(time.Now().Unix())),
		Min:    interval,
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.tryConnect(interval)
		}
	}
}

// tryConnect attempts to connect to a node and ping it. If successful, that node is marked healthy.
func (t *NodeHealthTracker) tryConnect(interval time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, t.connConfig)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	if err = conn.Ping(ctx); err != nil {
		return
	}
	log.Ctx(ctx).Trace().
		Uint32("nodeID", nodeID(conn)).
		Msg("health check connected to node")

	// nodes are marked healthy after a successful connection
	t.SetNodeHealth(nodeID(conn), true)
	t.Lock()
	defer t.Unlock()
	t.nodesEverSeen[nodeID(conn)] = t.newLimiter()
}

// SetNodeHealth marks a node as either healthy or unhealthy.
func (t *NodeHealthTracker) SetNodeHealth(nodeID uint32, healthy bool) {
	t.Lock()
	defer t.Unlock()
	defer func() {
		healthyCRDBNodeCountGauge.Set(float64(len(t.healthyNodes)))
	}()

	if _, ok := t.nodesEverSeen[nodeID]; !ok {
		t.nodesEverSeen[nodeID] = t.newLimiter()
	}

	if healthy {
		t.healthyNodes[nodeID] = struct{}{}
		t.nodesEverSeen[nodeID] = t.newLimiter()
		return
	}

	// If the limiter allows the request, it means we haven't seen more than
	// 2 failures in the past 1m, so the node shouldn't be marked unhealthy yet.
	// If the limiter denies the request, we've hit too many errors and the node
	// is marked unhealthy.
	if !t.nodesEverSeen[nodeID].Allow() {
		delete(t.healthyNodes, nodeID)
	}
}

// IsHealthy returns true if the given nodeID has been marked healthy.
func (t *NodeHealthTracker) IsHealthy(nodeID uint32) bool {
	t.RLock()
	_, ok := t.healthyNodes[nodeID]
	t.RUnlock()
	return ok
}

// HealthyNodeCount returns the number of healthy nodes currently tracked.
func (t *NodeHealthTracker) HealthyNodeCount() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.healthyNodes)
}
