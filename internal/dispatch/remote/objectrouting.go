package remote

import (
	"maps"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var ownerGroupsPerDispatch = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_owner_groups",
	Help:      "number of hashring owner groups a dispatched chunk was split into under object-affinity routing",
	Buckets:   []float64{1, 2, 3, 4, 5, 8, 12, 16, 32},
})

var ringFallbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "object_ring_fallback_total",
	Help:      "number of object-routed dispatches that fell back to first-ID routing because the hashring was unavailable",
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

func init() {
	prometheus.MustRegister(spreadEscalationsTotal)
	prometheus.MustRegister(spreadLatencyBlockedTotal)
	prometheus.MustRegister(ownerGroupsPerDispatch)
	prometheus.MustRegister(ringFallbackTotal)
}

// objectRoutingKey returns the routing key "<ns>/<objid>" for one object.
func objectRoutingKey(namespace, objectID string) []byte {
	key := make([]byte, 0, len(namespace)+len(objectID)+1)
	key = append(key, namespace...)
	key = append(key, '/')
	key = append(key, objectID...)
	return key
}

// ownerGroup is a set of resource IDs that one ring member owns.
type ownerGroup struct {
	ids []string
	// routingKey is an object key that the member of this owner group owns.
	routingKey []byte
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
func (cr *clusterDispatcher) passesShareTest(key []byte, call *spreadCall) bool {
	if cr.spreadShare <= 0 || cr.spread <= 1 || cr.spreadEstimator == nil {
		return false
	}
	count := cr.spreadEstimator.Touch(string(key))
	if count < minSpreadCount {
		return false
	}
	if !call.membersLoaded {
		call.members = len(cr.ringView.Members())
		call.membersLoaded = true
	}
	if call.members == 0 {
		return false
	}
	total := cr.spreadEstimator.Total()
	return float64(count) > cr.spreadShare/float64(call.members)*float64(total)
}

// latencyAllowsSpread reports whether the latency of owner permits a hot key of owner to spread.
// A factor of 0 or no latency tracking permits every spread.
func (cr *clusterDispatcher) latencyAllowsSpread(owner string, call *spreadCall) bool {
	if cr.spreadLatencyFactor <= 0 || cr.ownerLatency == nil {
		return true
	}
	if call.gate == nil {
		call.gate = cr.ownerLatency.gate()
	}
	return call.gate.allows(owner, cr.spreadLatencyFactor)
}

// saltKey returns key with the suffix "|salt".
// The salt selects one of spread ring positions at random.
// Thus the traffic and caches of a hot object spread across nodes.
// The result never aliases key.
func saltKey(key []byte, spread uint8) []byte {
	salted := make([]byte, len(key)+2)
	copy(salted, key)
	salted[len(key)] = '|'
	// nolint:gosec
	// G404: the salt only spreads load across peers. It needs no cryptographic randomness.
	salted[len(key)+1] = byte(rand.IntN(int(spread)))
	return salted
}

// ownerGroups splits resource IDs by ring owner.
// A hot key whose owner has elevated latency gets a salt, so its routing key can go to a different owner.
// ok=false means that the ring is unavailable, and ringFallbackTotal counts it.
// The caller must then route the chunk by the object key of its first ID.
// This reduces locality but is never an error.
func (cr *clusterDispatcher) ownerGroups(namespace string, ids []string) ([]ownerGroup, bool) {
	if cr.ringView == nil {
		ringFallbackTotal.Inc()
		return nil, false
	}
	var call spreadCall
	byOwner := make(map[string]int, 2)
	groups := make([]ownerGroup, 0, 2)
	for _, id := range ids {
		key := objectRoutingKey(namespace, id)
		hot := cr.passesShareTest(key, &call)
		members, err := cr.ringView.FindN(key, 1)
		if err != nil || len(members) == 0 {
			ringFallbackTotal.Inc()
			return nil, false
		}
		if hot {
			if cr.latencyAllowsSpread(members[0].Key(), &call) {
				spreadEscalationsTotal.Inc()
				key = saltKey(key, cr.spread)
				members, err = cr.ringView.FindN(key, 1)
				if err != nil || len(members) == 0 {
					ringFallbackTotal.Inc()
					return nil, false
				}
			} else {
				spreadLatencyBlockedTotal.Inc()
			}
		}
		owner := members[0].Key()
		idx, ok := byOwner[owner]
		if !ok {
			idx = len(groups)
			byOwner[owner] = idx
			groups = append(groups, ownerGroup{routingKey: key, owner: owner})
		}
		groups[idx].ids = append(groups[idx].ids, id)
	}
	ownerGroupsPerDispatch.Observe(float64(len(groups)))
	return groups, true
}

// primaryLatencyObserver returns a function that records a primary dispatch latency for owner.
// It returns nil when latency tracking is off or the owner is unknown.
// The owner is the first ring member for the routing key.
// The balancer sends the RPC to that member only when the hashring spread is 1 (--dispatch-hashring-spread=1).
// With a larger hashring spread, the latency of another member can count for the owner.
func (cr *clusterDispatcher) primaryLatencyObserver(owner string) func(time.Duration) {
	if cr.ownerLatency == nil || owner == "" {
		return nil
	}
	return func(latency time.Duration) { cr.ownerLatency.record(owner, latency) }
}

// mergeCheckResponses combines the responses of disjoint owner groups.
// It adds the dispatch counts and keeps the maximum DepthRequired, because the owner groups run in parallel.
// DebugInfo stays nil because debug requests never fan out.
func mergeCheckResponses(responses []*v1.DispatchCheckResponse) *v1.DispatchCheckResponse {
	merged := &v1.DispatchCheckResponse{
		Metadata:            &v1.ResponseMeta{},
		ResultsByResourceId: make(map[string]*v1.ResourceCheckResult),
	}
	for _, resp := range responses {
		if resp == nil {
			continue
		}
		maps.Copy(merged.ResultsByResourceId, resp.ResultsByResourceId)
		if resp.Metadata == nil {
			continue
		}
		merged.Metadata.DispatchCount += resp.Metadata.DispatchCount
		merged.Metadata.CachedDispatchCount += resp.Metadata.CachedDispatchCount
		merged.Metadata.DepthRequired = max(merged.Metadata.DepthRequired, resp.Metadata.DepthRequired)
	}
	return merged
}
