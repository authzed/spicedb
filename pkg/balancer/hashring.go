package balancer

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/authzed/spicedb/pkg/consistent"
)

// This is based off of the example implementation in grpc-go:
//   https://github.com/grpc/grpc-go/blob/afcbdc9ace7b4af94d014620727ea331cc3047fe/balancer/base/balancer.go
// The original work is copyright gRPC authors and licensed under the Apache License, Version 2.0.

// Note that there is little locking in this file. In grpc it is wrapped
// with `ccBalancerWrapper` which serializes calls to these callbacks.
//  See: https://github.com/grpc/grpc-go/blob/417d4b6895679bd9378cb37c2afecf6a292eb267/balancer_conn_wrappers.go#L35-L48
// The hashring also locks internally.a

type ctxKey string

const (
	// BalancerName is the name of consistent-hashring balancer.
	BalancerName = "consistent-hashring"

	// CtxKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	CtxKey ctxKey = "requestKey"

	defaultReplicationFactor = 100
	defaultSpread            = 1
)

var defaultBalancerServiceConfig = &ConsistentHashringBalancerConfig{
	ReplicationFactor: defaultReplicationFactor,
	Spread:            defaultSpread,
}

// DefaultBalancerServiceConfigJSON is a grpc Service Config JSON with the
// defaults for the ConsstentHashringBalancer configured.
var DefaultBalancerServiceConfigJSON = defaultBalancerServiceConfig.MustToServiceConfigJSON()

// ConsistentHashringBalancerConfig supports common settings for the balancer.
// It should be converted to json with ToServiceConfigJSON or
// MustToServiceConfigJSON and passed to the grpc.WithDefaultServiceConfig
// option on grpc.Dial.
type ConsistentHashringBalancerConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`
	ReplicationFactor                 uint16 `json:"replicationFactor,omitempty"`
	Spread                            uint8  `json:"spread,omitempty"`
}

// ToServiceConfigJSON converts the config into the standard grpc Service Config
// json format.
func (c *ConsistentHashringBalancerConfig) ToServiceConfigJSON() (string, error) {
	type wrapper struct {
		Config []map[string]*ConsistentHashringBalancerConfig `json:"loadBalancingConfig"`
	}

	out := wrapper{Config: []map[string]*ConsistentHashringBalancerConfig{{
		BalancerName: c,
	}}}

	j, err := json.Marshal(out)
	if err != nil {
		return "", err
	}

	return string(j), nil
}

// MustToServiceConfigJSON calls ToServiceConfigJSON but panics if there is an
// error.
func (c *ConsistentHashringBalancerConfig) MustToServiceConfigJSON() string {
	o, err := c.ToServiceConfigJSON()
	if err != nil {
		panic(err)
	}

	return o
}

var logger = grpclog.Component("consistenthashring")

// NewConsistentHashringBuilder creates a new balancer.Builder that
// will create a consistent hashring balancer.
// Before making a connection, register it with grpc with:
// `balancer.Register(consistent.NewConsistentHashringBuilder(hasher))`
func NewConsistentHashringBuilder(hasher consistent.HasherFunc) *ConsistentHashringBuilder {
	return &ConsistentHashringBuilder{
		hasher: hasher,
	}
}

type subConnMember struct {
	balancer.SubConn
	key string
}

// Key implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (s subConnMember) Key() string {
	return s.key
}

var _ consistent.Member = &subConnMember{}

// ConsistentHashringBuilder stamps out new ConsistentHashringBalancer
// when requested by grpc.
type ConsistentHashringBuilder struct {
	sync.Mutex
	hasher consistent.HasherFunc
	config ConsistentHashringBalancerConfig
}

// Build satisfies balancer.Builder and returns a new ConsistentHashringBalancer.
func (b *ConsistentHashringBuilder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	bal := &ConsistentHashringBalancer{
		cc:       cc,
		subConns: resolver.NewAddressMap(),
		scStates: make(map[balancer.SubConn]connectivity.State),
		csEvltr:  &balancer.ConnectivityStateEvaluator{},
		state:    connectivity.Connecting,
		hasher:   b.hasher,
		picker:   base.NewErrPicker(balancer.ErrNoSubConnAvailable),
	}

	return bal
}

// Name satisfies balancer.Builder and returns the name of the balancer for
// use in Service Config files.
func (b *ConsistentHashringBuilder) Name() string {
	return BalancerName
}

// ParseConfig satisfies balancer.ConfigParser and is used to parse new
// Service Config json. The results are stored on the builder so that
// subsequently built Balancers use the config.
func (b *ConsistentHashringBuilder) ParseConfig(js json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var lbCfg ConsistentHashringBalancerConfig
	if err := json.Unmarshal(js, &lbCfg); err != nil {
		return nil, fmt.Errorf("wrr: unable to unmarshal LB policy config: %s, error: %w", string(js), err)
	}

	logger.Infof("parsed balancer config %s", js)

	if lbCfg.ReplicationFactor == 0 {
		lbCfg.ReplicationFactor = defaultReplicationFactor
	}

	if lbCfg.Spread == 0 {
		lbCfg.Spread = defaultSpread
	}

	b.Lock()
	b.config = lbCfg
	b.Unlock()

	return &lbCfg, nil
}

// ConsistentHashringBalancer implements balancer.Balancer and uses a
// consistent hashring to pick a backend for a request.
type ConsistentHashringBalancer struct {
	state    connectivity.State
	cc       balancer.ClientConn
	picker   balancer.Picker
	csEvltr  *balancer.ConnectivityStateEvaluator
	subConns *resolver.AddressMap
	scStates map[balancer.SubConn]connectivity.State

	config   *ConsistentHashringBalancerConfig
	hashring *consistent.Hashring
	hasher   consistent.HasherFunc

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

// ResolverError satisfies balancer.Balancer and is called when there is a
// an error in the resolver.
func (b *ConsistentHashringBalancer) ResolverError(err error) {
	b.resolverErr = err
	if b.subConns.Len() == 0 {
		b.state = connectivity.TransientFailure
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}

	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// UpdateClientConnState satisfies balancer.Balancer and is called when there
// are changes in the Address set or Service Config that the balancer may
// want to react to. In this case, the hashring is updated and a new picker
// using that hashring is generated.
func (b *ConsistentHashringBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	if logger.V(2) {
		logger.Info("got new ClientConn state: ", s)
	}
	// Successful resolution: clear resolver error and ensure we return nil.
	b.resolverErr = nil

	// update the service config if it has changed
	if s.BalancerConfig != nil {
		svcConfig := s.BalancerConfig.(*ConsistentHashringBalancerConfig)
		if b.config == nil || svcConfig.ReplicationFactor != b.config.ReplicationFactor {
			b.hashring = consistent.MustNewHashring(b.hasher, svcConfig.ReplicationFactor)
			b.config = svcConfig
		}
	}

	// if there's no hashring yet, the balancer hasn't yet parsed an initial
	// service config with settings
	if b.hashring == nil {
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
		b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

		return fmt.Errorf("no hashring configured")
	}

	// Look through the set of addresses the resolver has passed to the balancer
	// if any new targets have been added, they are added to the hashring, and
	// any that have been removed since the last update are removed from the
	// hashring.
	addrsSet := resolver.NewAddressMap()
	for _, addr := range s.ResolverState.Addresses {
		addrsSet.Set(addr, nil)

		if _, ok := b.subConns.Get(addr); !ok {
			// addr is addr new address (not existing in b.subConns).
			sc, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				logger.Warningf("base.baseBalancer: failed to create new SubConn: %v", err)
				continue
			}

			b.subConns.Set(addr, sc)
			b.scStates[sc] = connectivity.Idle
			b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)
			sc.Connect()

			if err := b.hashring.Add(subConnMember{
				SubConn: sc,
				key:     addr.ServerName + addr.Addr,
			}); err != nil {
				return fmt.Errorf("couldn't add to hashring")
			}
		}
	}

	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		// addr was removed by resolver.
		if _, ok := addrsSet.Get(addr); !ok {
			b.cc.RemoveSubConn(sc)
			b.subConns.Delete(addr)
			// Keep the state of this sc in b.scStates until sc's state becomes Shutdown.
			// The entry will be deleted in UpdateSubConnState.
			if err := b.hashring.Remove(subConnMember{
				SubConn: sc,
				key:     addr.ServerName + addr.Addr,
			}); err != nil {
				return fmt.Errorf("couldn't add to hashring")
			}
		}
	}

	if logger.V(2) {
		logger.Infof("%d hashring members found", len(b.hashring.Members()))

		for _, m := range b.hashring.Members() {
			logger.Infof("hashring member %s", m.Key())
		}
	}

	// If resolver state contains no addresses, return an error so ClientConn
	// will trigger re-resolve. Also records this as addr resolver error, so when
	// the overall state turns transient failure, the error message will have
	// the zero address information.
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// If the overall connection state is not in transient failure, we return
	// addr new picker with addr reference to the hashring (otherwise an error picker)
	if b.state == connectivity.TransientFailure {
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
	} else {
		b.picker = &consistentHashringPicker{
			hashring: b.hashring,
			spread:   b.config.Spread,
			rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		}
	}

	// update the ClientConn with the current hashring picker picker
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

	return nil
}

// UpdateSubConnState satisfies balancer.Balancer and is called when there is
// a change in a subconnection state. Subconnection state can affect the overall
// state of the balancer. This also attempts to reconnect any idle connections.
func (b *ConsistentHashringBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	if logger.V(2) {
		logger.Infof("base.baseBalancer: handle SubConn state change: %p, %v", sc, s)
	}

	oldS, ok := b.scStates[sc]
	if !ok {
		if logger.V(2) {
			logger.Infof("base.baseBalancer: got state changes for an unknown SubConn: %p, %v", sc, s)
		}

		return
	}

	if oldS == connectivity.TransientFailure &&
		(s == connectivity.Connecting || s == connectivity.Idle) {
		// Once a subconn enters TRANSIENT_FAILURE, ignore subsequent IDLE or
		// CONNECTING transitions to prevent the aggregated state from being
		// always CONNECTING when many backends exist but are all down.
		if s == connectivity.Idle {
			sc.Connect()
		}

		return
	}

	b.scStates[sc] = s

	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		// When an address was removed by resolver, b called RemoveSubConn but
		// kept the sc's state in scStates. Remove state for this sc here.
		delete(b.scStates, sc)
	case connectivity.TransientFailure:
		// Save error to be reported via picker.
		b.connErr = state.ConnectionError
	}

	b.state = b.csEvltr.RecordTransition(oldS, s)

	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// Close is a no-op because base balancer doesn't have internal state to clean up,
// and it doesn't need to call RemoveSubConn for the SubConns.
func (b *ConsistentHashringBalancer) Close() {
}

type consistentHashringPicker struct {
	sync.Mutex
	hashring *consistent.Hashring
	spread   uint8
	rand     *rand.Rand
}

// Pick satisfies balancer.Picker and returns a subconnection to use for a
// request based on the request info. The value stored in CtxKey is hashed
// into the hashring, and the resulting subconnection is used. Note that
// there is no fallback behavior if the subconnection is unavailable; this
// prevents the request from going to a node that doesn't expect to receive it.
// Spread can be increased to be robust against single node availability
// problems.
// For dispatch we generally use a resolver that removes connections from the
// list when they are observably unavailable, so in practice this is not a
// huge problem.
// If spread is greater than 1, a random selection is made from the set of
// subconns matching the hash.
func (p *consistentHashringPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	key := info.Ctx.Value(CtxKey).([]byte)

	members, err := p.hashring.FindN(key, p.spread)
	if err != nil {
		return balancer.PickResult{}, err
	}

	index := 0

	if p.spread > 1 {
		// TODO: should look into other options for this to avoid locking; we mostly use spread 1 so it's not urgent
		// rand is not safe for concurrent use
		p.Lock()
		index = p.rand.Intn(int(p.spread))
		p.Unlock()
	}

	chosen := members[index].(subConnMember)

	return balancer.PickResult{
		SubConn: chosen.SubConn,
	}, nil
}
