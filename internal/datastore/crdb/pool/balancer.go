package pool

import (
	"context"
	"hash/maphash"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/semaphore"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/genutil"
)

var (
	connectionsPerCRDBNodeCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "crdb_connections_per_node",
		Help: "the number of connections spicedb has to each crdb node",
	}, []string{"pool", "node_id"})

	pruningTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "crdb_pruning_duration",
		Help:    "milliseconds spent on one iteration of pruning excess connections",
		Buckets: []float64{.1, .2, .5, 1, 2, 5, 10, 20, 50, 100},
	}, []string{"pool"})
)

func init() {
	prometheus.MustRegister(connectionsPerCRDBNodeCountGauge)
	prometheus.MustRegister(pruningTimeHistogram)
}

type balancePoolConn[C balanceConn] interface {
	Conn() C
	Release()
}

type balanceConn interface {
	comparable
	IsClosed() bool
}

// balanceablePool is an interface that a pool must implement to allow its
// connections to be balanced by the balancer.
type balanceablePool[P balancePoolConn[C], C balanceConn] interface {
	ID() string
	AcquireAllIdle(ctx context.Context) []P
	Node(conn C) uint32
	GC(conn C)
	MaxConns() uint32
	Range(func(conn C, nodeID uint32))
}

// NodeConnectionBalancer attempts to keep the connections managed by a RetryPool balanced between healthy nodes in
// a Cockroach cluster.
// It asynchronously processes idle connections, and kills any to nodes that have too many. When the pool reconnects,
// it will have a different balance of connections, and over time the balancer will bring the counts close to equal.
type NodeConnectionBalancer struct {
	nodeConnectionBalancer[*pgxpool.Conn, *pgx.Conn]
}

// NewNodeConnectionBalancer builds a new nodeConnectionBalancer for a given connection pool and health tracker.
func NewNodeConnectionBalancer(pool *RetryPool, healthTracker *NodeHealthTracker, interval time.Duration) *NodeConnectionBalancer {
	return &NodeConnectionBalancer{*newNodeConnectionBalancer[*pgxpool.Conn, *pgx.Conn](pool, healthTracker, interval)}
}

// nodeConnectionBalancer is generic over underlying connection types for
// testing purposes. Callers should use the exported NodeConnectionBalancer
type nodeConnectionBalancer[P balancePoolConn[C], C balanceConn] struct {
	ticker        *time.Ticker
	sem           *semaphore.Weighted
	pool          balanceablePool[P, C]
	healthTracker *NodeHealthTracker
	rnd           *rand.Rand
	seed          int64
}

// newNodeConnectionBalancer is generic over underlying connection types for
// testing purposes. Callers should use the exported NewNodeConnectionBalancer.
func newNodeConnectionBalancer[P balancePoolConn[C], C balanceConn](pool balanceablePool[P, C], healthTracker *NodeHealthTracker, interval time.Duration) *nodeConnectionBalancer[P, C] {
	seed := int64(0)
	for seed == 0 {
		// Sum64 returns a uint64, and safecast will return 0 if it's not castable,
		// which will happen about half the time (?). We just keep running it until
		// we get a seed that fits in the box.
		// Subtracting math.MaxInt64 should mean that we retain the entire range of
		// possible values.
		seed, _ = safecast.ToInt64(new(maphash.Hash).Sum64() - math.MaxInt64)
	}
	return &nodeConnectionBalancer[P, C]{
		ticker:        time.NewTicker(interval),
		sem:           semaphore.NewWeighted(1),
		healthTracker: healthTracker,
		pool:          pool,
		seed:          seed,
		// nolint:gosec
		// use of non cryptographically secure random number generator is not concern here,
		// as it's used for shuffling the nodes to balance the connections when the number of
		// connections do not divide evenly.
		rnd: rand.New(rand.NewSource(seed)),
	}
}

// Prune starts periodically checking idle connections and killing ones that are determined to be unbalanced.
func (p *nodeConnectionBalancer[P, C]) Prune(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.ticker.Stop()
			return
		case <-p.ticker.C:
			if p.sem.TryAcquire(1) {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				p.mustPruneConnections(ctx)
				cancel()
				p.sem.Release(1)
			}
		}
	}
}

// mustPruneConnections prunes connections to nodes that have more than MaxConns/(# of nodes)
// This causes the pool to reconnect, which over time will lead to a balanced number of connections
// across each node.
func (p *nodeConnectionBalancer[P, C]) mustPruneConnections(ctx context.Context) {
	start := time.Now()
	defer func() {
		pruningTimeHistogram.WithLabelValues(p.pool.ID()).Observe(float64(time.Since(start).Milliseconds()))
	}()
	conns := p.pool.AcquireAllIdle(ctx)
	defer func() {
		// release all acquired idle conns back
		for _, c := range conns {
			c.Release()
		}
	}()

	// bucket connections by healthy node
	healthyConns := make(map[uint32][]P, 0)
	for _, c := range conns {
		id := p.pool.Node(c.Conn())
		if p.healthTracker.IsHealthy(id) {
			if healthyConns[id] == nil {
				healthyConns[id] = make([]P, 0, 1)
			}
			healthyConns[id] = append(healthyConns[id], c)
		} else {
			p.pool.GC(c.Conn())
		}
	}

	// It's highly unlikely that we'll ever have an overflow in
	// this context, so we cast directly.
	nodeCount, _ := safecast.ToUint32(p.healthTracker.HealthyNodeCount())
	if nodeCount == 0 {
		nodeCount = 1
	}

	connectionCounts := make(map[uint32]uint32)
	p.pool.Range(func(conn C, nodeID uint32) {
		connectionCounts[nodeID]++
	})

	log.Ctx(ctx).Trace().
		Str("pool", p.pool.ID()).
		Any("counts", connectionCounts).
		Msg("connections per node")

	// Delete metrics for nodes we no longer have connections for
	p.healthTracker.RLock()
	for node := range p.healthTracker.nodesEverSeen {
		if _, ok := connectionCounts[node]; !ok {
			connectionsPerCRDBNodeCountGauge.DeletePartialMatch(map[string]string{
				"pool":    p.pool.ID(),
				"node_id": strconv.FormatUint(uint64(node), 10),
			})
		}
	}
	p.healthTracker.RUnlock()

	nodes := maps.Keys(connectionCounts)
	slices.Sort(nodes)

	// Shuffle nodes in place deterministically based on the initial seed.
	// This will always generate the same distribution for the life of the
	// program, but prevents the same nodes from getting all the "extra"
	// connections when they don't divide evenly over nodes.
	p.rnd.Seed(p.seed)
	p.rnd.Shuffle(len(nodes), func(i, j int) {
		nodes[j], nodes[i] = nodes[i], nodes[j]
	})

	initialPerNodeMax := p.pool.MaxConns() / nodeCount
	for i, node := range nodes {
		count := connectionCounts[node]
		connectionsPerCRDBNodeCountGauge.WithLabelValues(
			p.pool.ID(),
			strconv.FormatUint(uint64(node), 10),
		).Set(float64(count))

		perNodeMax := initialPerNodeMax

		// Assign MaxConns%(# of nodes) nodes an extra connection. This ensures that
		// the sum of all perNodeMax values exactly equals the pool MaxConns.
		// Without this, we will either over or underestimate the perNodeMax.
		// If we underestimate, the balancer will fight the pool, and if we overestimate,
		// it's possible for the difference in connections between nodes to differ by up to
		// the number of nodes.
		if p.healthTracker.HealthyNodeCount() == 0 ||
			i < int(p.pool.MaxConns())%p.healthTracker.HealthyNodeCount() {
			perNodeMax++
		}

		// Need to remove excess connections above the perNodeMax
		numToPrune := count - perNodeMax

		if count <= perNodeMax {
			continue
		}
		log.Ctx(ctx).Trace().
			Uint32("node", node).
			Uint32("poolmaxconns", p.pool.MaxConns()).
			Uint32("conncount", count).
			Uint32("nodemaxconns", perNodeMax).
			Msg("node connections require pruning")

		// Prune half of the distance we're trying to cover. This will prune more connections if the gap between
		// desired and target is large.
		if numToPrune > 1 {
			numToPrune >>= 1
		}

		healthyNodeCount := genutil.MustEnsureUInt32(len(healthyConns[node]))
		if healthyNodeCount < numToPrune {
			numToPrune = healthyNodeCount
		}
		if numToPrune == 0 {
			continue
		}

		for _, c := range healthyConns[node][:numToPrune] {
			log.Ctx(ctx).Trace().Str("pool", p.pool.ID()).Uint32("node", node).Msg("pruning connection")
			p.pool.GC(c.Conn())
		}

		log.Ctx(ctx).Trace().Str("pool", p.pool.ID()).Uint32("node", node).Uint32("prunedCount", numToPrune).Msg("pruned connections")
	}
}
