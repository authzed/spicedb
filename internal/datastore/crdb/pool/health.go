package pool

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lthibault/jitterbug"
	"github.com/prometheus/client_golang/prometheus"

	log "github.com/authzed/spicedb/internal/logging"
)

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
	sync.Mutex
	connConfig    *pgx.ConnConfig
	healthyNodes  map[uint32]struct{}
	nodesEverSeen map[uint32]struct{}
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
		nodesEverSeen: make(map[uint32]struct{}, 0),
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
	log.Ctx(ctx).Info().
		Uint32("nodeID", nodeID(conn)).
		Msg("health check connected to node")

	// nodes are marked healthy after a successful connection
	t.SetNodeHealth(nodeID(conn), true)
	t.Lock()
	defer t.Unlock()
	t.nodesEverSeen[nodeID(conn)] = struct{}{}
}

// SetNodeHealth marks a node as either healthy or unhealthy.
func (t *NodeHealthTracker) SetNodeHealth(nodeID uint32, healthy bool) {
	t.Lock()
	defer t.Unlock()
	defer func() {
		healthyCRDBNodeCountGauge.Set(float64(len(t.healthyNodes)))
	}()
	if healthy {
		t.healthyNodes[nodeID] = struct{}{}
		return
	}
	delete(t.healthyNodes, nodeID)
}

// IsHealthy returns true if the given nodeID has been marked healthy.
func (t *NodeHealthTracker) IsHealthy(nodeID uint32) bool {
	t.Lock()
	_, ok := t.healthyNodes[nodeID]
	t.Unlock()
	return ok
}

// HealthyNodeCount returns the number of healthy nodes currently tracked.
func (t *NodeHealthTracker) HealthyNodeCount() int {
	t.Lock()
	defer t.Unlock()
	return len(t.healthyNodes)
}
