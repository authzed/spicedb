package remote

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/authzed/consistent"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/frequency"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var ownerGroupsPerChunkCall = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_owner_groups",
	Help:      "number of hashring owner groups in one call of the object routing chunker",
	Buckets:   []float64{1, 2, 3, 4, 5, 8, 12, 16, 32},
})

var ringFallbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_ring_fallback_total",
	Help:      "number of routing chunker calls that used size-only chunks routed by first ID because the hashring was unavailable",
})

var spreadEscalationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_spread_escalations_total",
	Help:      "number of object routing keys salted into a spread position because they passed the share-of-traffic test and the owner-latency gate",
})

var spreadLatencyBlockedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_spread_latency_blocked_total",
	Help:      "number of object routing keys that passed the share-of-traffic test but did not spread because the latency of their owner was not elevated",
})

// minSpreadCount is the minimum decayed count of a key before it can spread.
// Thus a quiet node does not spread its first few keys.
const minSpreadCount = 100

// spreadWindow is the decay window of the spread estimator and the window of the owner latency digests.
const spreadWindow = time.Minute

// spreadEstimatorBudget is the memory budget in bytes of the spread estimator.
const spreadEstimatorBudget = 8 << 20

func init() {
	prometheus.MustRegister(spreadEscalationsTotal)
	prometheus.MustRegister(spreadLatencyBlockedTotal)
	prometheus.MustRegister(ownerGroupsPerChunkCall)
	prometheus.MustRegister(ringFallbackTotal)
}

// objectRouter routes each dispatch by the ring owner of its resource objects.
// A dispatch without a routing hint uses the fallback router.
type objectRouter struct {
	fallback router

	// ringView is nil when no ring view is available.
	ringView consistent.RingView

	// spreadEstimator is nil when spread is disabled.
	spreadEstimator frequency.Estimator

	// ownerLatency is nil when spread or the latency gate is disabled.
	ownerLatency *ownerLatencies

	// spreadShare is the share-of-traffic threshold for spread. Zero disables spread.
	spreadShare float64

	// spread is the number of ring owners for a hot key.
	spread uint8

	// spreadLatencyFactor is the owner-latency gate factor for spread. Zero disables the gate.
	spreadLatencyFactor float64
}

var _ router = (*objectRouter)(nil)

// newObjectRouter returns an objectRouter that uses fallback for dispatches without a routing hint.
// It gets the ring view for conn from config.HashringBuilder.
// An estimator failure only disables spread. It never fails the router.
func newObjectRouter(fallback router, conn *grpc.ClientConn, config ClusterDispatcherConfig) *objectRouter {
	var ringView consistent.RingView
	if config.HashringBuilder != nil && conn != nil {
		// gRPC gives conn.CanonicalTarget() to Builder.Build, so the ring view and the picker share one slot.
		ringView = config.HashringBuilder.RingFor(conn.CanonicalTarget())
		log.Info().Str("target", conn.CanonicalTarget()).
			Float64("spread_share", config.SpreadShare).
			Float64("spread_latency_factor", config.SpreadLatencyFactor).
			Msg("object-affinity routing enabled")
	} else {
		log.Warn().Bool("has_hashring_builder", config.HashringBuilder != nil).Bool("has_conn", conn != nil).
			Msg("object-affinity routing has no hashring view, so each dispatch routes by its first resource ID")
	}

	var spreadEstimator frequency.Estimator
	if config.SpreadShare > 0 {
		est, err := frequency.NewEstimator(spreadEstimatorBudget, spreadWindow)
		if err != nil {
			log.Warn().Err(err).Msg("cannot create the object spread estimator, so per-key spread is off")
		} else {
			spreadEstimator = est
		}
	}

	var ownerLatency *ownerLatencies
	if spreadEstimator != nil && config.SpreadLatencyFactor > 0 {
		ownerLatency = newOwnerLatencies(spreadWindow, time.Now)
	}

	return &objectRouter{
		fallback:            fallback,
		ringView:            ringView,
		spreadEstimator:     spreadEstimator,
		ownerLatency:        ownerLatency,
		spreadShare:         config.SpreadShare,
		spread:              config.Spread,
		spreadLatencyFactor: config.SpreadLatencyFactor,
	}
}

// checkKey returns the key and the owner of the routing hint in ctx.
// Without a hint, it returns the result of the fallback router.
func (r *objectRouter) checkKey(ctx context.Context, req *v1.DispatchCheckRequest) (dispatch.RoutingKey, string, error) {
	if hint, ok := dispatch.RoutingHintFrom(ctx); ok {
		return hint.Key, hint.Owner, nil
	}
	return r.fallback.checkKey(ctx, req)
}

// lookupSubjectsKey returns the key and the owner of the routing hint in ctx.
// Without a hint, it returns the result of the fallback router.
func (r *objectRouter) lookupSubjectsKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (dispatch.RoutingKey, string, error) {
	if hint, ok := dispatch.RoutingHintFrom(ctx); ok {
		return hint.Key, hint.Owner, nil
	}
	return r.fallback.lookupSubjectsKey(ctx, req)
}

// latencyObserver returns a function that records a primary dispatch latency for owner.
// It returns nil when latency tracking is off or the owner is unknown.
// The owner is the first ring member for the routing key.
// The balancer sends the RPC to that member only when the hashring spread is 1 (--dispatch-hashring-spread=1).
// With a larger hashring spread, the latency of another member can count for the owner.
func (r *objectRouter) latencyObserver(owner string) func(time.Duration) {
	if r.ownerLatency == nil || owner == "" {
		return nil
	}
	return func(latency time.Duration) { r.ownerLatency.record(owner, latency) }
}

// Close closes the spread estimator.
func (r *objectRouter) Close() {
	if r.spreadEstimator != nil {
		r.spreadEstimator.Close()
	}
}

// saltSeparator separates a routing key from its salt.
// A namespace or an object ID cannot contain it, and the relationship syntax does not use it.
const saltSeparator = '!'

// objectRoutingKey returns the routing key "<ns>:<objid>" for one object.
// A namespace or an object ID cannot contain ':' or saltSeparator.
// Thus distinct objects have distinct keys, and no key equals a key from saltKey.
// A collision changes only the routing, not the results.
func objectRoutingKey(namespace, objectID string) dispatch.RoutingKey {
	key := make(dispatch.RoutingKey, 0, len(namespace)+len(objectID)+1)
	key = append(key, namespace...)
	key = append(key, ':')
	key = append(key, objectID...)
	return key
}

// ownerGroup is a set of resource IDs that one ring member owns.
type ownerGroup struct {
	ids []string

	// routingKey is an object key that the member of this owner group owns.
	routingKey dispatch.RoutingKey

	// owner is the ring member key of the member of this owner group.
	owner string
}

// spreadCall holds values that one ownerGroups call loads only when a key needs them.
type spreadCall struct {
	members       int
	membersLoaded bool

	// gate is nil until a hot key needs it.
	gate *latencyGate
}

// passesShareTest records one dispatch of key and reports whether key is hot.
// A key is hot when its decayed count is at least minSpreadCount
// and its share of the outbound dispatches of this node is more than spreadShare divided by the ring member count.
func (r *objectRouter) passesShareTest(key dispatch.RoutingKey, call *spreadCall) bool {
	if r.spreadShare <= 0 || r.spread <= 1 || r.spreadEstimator == nil {
		return false
	}

	count := r.spreadEstimator.Touch(key)
	if count < minSpreadCount {
		return false
	}

	if !call.membersLoaded {
		call.members = len(r.ringView.Members())
		call.membersLoaded = true
	}

	if call.members == 0 {
		return false
	}

	total := r.spreadEstimator.Total()
	return float64(count) > r.spreadShare/float64(call.members)*float64(total)
}

// latencyAllowsSpread reports whether the latency of owner permits a hot key of owner to spread.
// A factor of 0 or no latency tracking permits every spread.
func (r *objectRouter) latencyAllowsSpread(owner string, call *spreadCall) bool {
	if r.spreadLatencyFactor <= 0 || r.ownerLatency == nil {
		return true
	}

	if call.gate == nil {
		call.gate = r.ownerLatency.gate()
	}
	return call.gate.allows(owner, r.spreadLatencyFactor)
}

// saltKey returns key with the suffix "!salt".
// The salt selects one of the spread ring positions at random.
// Thus the traffic and caches of a hot object spread across nodes.
// The result never aliases key.
func saltKey(key dispatch.RoutingKey, spread uint8) dispatch.RoutingKey {
	salted := make(dispatch.RoutingKey, len(key)+2)
	copy(salted, key)
	salted[len(key)] = saltSeparator
	// nolint:gosec
	// G404: the salt only spreads load across peers. It needs no cryptographic randomness.
	salted[len(key)+1] = byte(rand.IntN(int(spread)))
	return salted
}

// ownerGroups splits resource IDs by ring owner.
// A hot key whose owner has elevated latency gets a salt, so its routing key can go to a different owner.
// ok=false means that the ring is unavailable, and ringFallbackTotal counts it.
// The caller then chunks the IDs by size and routes each chunk by the object key of its first ID.
// This reduces locality but is never an error.
func (r *objectRouter) ownerGroups(ctx context.Context, namespace string, ids []string) ([]ownerGroup, bool) {
	if r.ringView == nil {
		ringFallbackTotal.Inc()
		log.Ctx(ctx).Trace().Str("namespace", namespace).Msg("object routing ring fallback: no ring view")
		return nil, false
	}

	var call spreadCall
	byOwner := make(map[string]int, 2)
	groups := make([]ownerGroup, 0, 2)
	for _, id := range ids {
		// Count the dispatch of the object key of id, and find the owner of the key.
		key := objectRoutingKey(namespace, id)
		hot := r.passesShareTest(key, &call)

		owner, ok := r.ringOwner(ctx, key)
		if !ok {
			return nil, false
		}

		// A hot key spreads only if the latency of its owner is elevated.
		// The salted key selects a spread ring position, which can have a different owner.
		if hot {
			if r.latencyAllowsSpread(owner, &call) {
				spreadEscalationsTotal.Inc()
				key = saltKey(key, r.spread)
				log.Ctx(ctx).Trace().Bytes("key", key).Str("owner", owner).Msg("object routing key spreads")

				owner, ok = r.ringOwner(ctx, key)
				if !ok {
					return nil, false
				}
			} else {
				spreadLatencyBlockedTotal.Inc()
				log.Ctx(ctx).Trace().Bytes("key", key).Str("owner", owner).Msg("object routing hot key blocked by latency gate")
			}
		}

		// Add id to the group of its owner. The key of the first ID of a group is the routing key of the group.
		idx, found := byOwner[owner]
		if !found {
			idx = len(groups)
			byOwner[owner] = idx
			groups = append(groups, ownerGroup{routingKey: key, owner: owner})
		}

		groups[idx].ids = append(groups[idx].ids, id)
	}

	ownerGroupsPerChunkCall.Observe(float64(len(groups)))
	return groups, true
}

// ringOwner returns the ring member key of the owner of key.
// ok=false means that the ring has no owner for key, and ringFallbackTotal counts it.
func (r *objectRouter) ringOwner(ctx context.Context, key dispatch.RoutingKey) (string, bool) {
	members, err := r.ringView.FindN(key, 1)
	if err != nil || len(members) == 0 {
		ringFallbackTotal.Inc()
		log.Ctx(ctx).Trace().Err(err).Bytes("key", key).Msg("object routing ring fallback")
		return "", false
	}

	return members[0].Key(), true
}

// Chunk groups ids by ring owner and splits each owner group into chunks of at most maxChunkSize IDs.
// The groups are in order of first appearance, and each group keeps the input order.
// The hint of each chunk has the routing key and the owner of its group.
// If the ring is unavailable, Chunk splits ids by size only, and each chunk routes by the object key of its first ID.
func (r *objectRouter) Chunk(ctx context.Context, namespace string, ids []string, maxChunkSize uint16) []dispatch.Chunk {
	if len(ids) == 0 {
		return nil
	}

	groups, ok := r.ownerGroups(ctx, namespace, ids)
	if !ok {
		chunks := dispatch.DefaultChunker.Chunk(ctx, namespace, ids, maxChunkSize)
		for i := range chunks {
			chunks[i].Hint = dispatch.RoutingHint{Key: objectRoutingKey(namespace, chunks[i].ResourceIDs[0])}
		}

		return chunks
	}

	chunks := make([]dispatch.Chunk, 0, len(groups))
	for _, group := range groups {
		hint := dispatch.RoutingHint{Key: group.routingKey, Owner: group.owner}
		slicez.ForEachChunk(group.ids, maxChunkSize, func(part []string) {
			chunks = append(chunks, dispatch.Chunk{ResourceIDs: part, Hint: hint})
		})
	}

	return chunks
}
