package balancer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"

	"github.com/authzed/spicedb/pkg/consistent"
)

type ctxKey string

const (
	// BalancerName is the name of consistent-hashring balancer.
	BalancerName = "consistent-hashring"

	// BalancerServiceConfig is a service config that sets the default balancer
	// to the consistent-hashring balancer
	BalancerServiceConfig = `{"loadBalancingPolicy":"consistent-hashring"}`

	// CtxKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	CtxKey ctxKey = "requestKey"
)

var logger = grpclog.Component("consistenthashring")

// NewConsistentHashringBuilder creates a new balancer.Builder that
// will create a consistent hashring balancer with the picker builder.
// Before making a connection, register it with grpc with:
// `balancer.Register(consistent.NewConsistentHashringBuilder(hasher, factor, spread))`
func NewConsistentHashringBuilder(pickerBuilder base.PickerBuilder) balancer.Builder {
	return base.NewBalancerBuilder(
		BalancerName,
		pickerBuilder,
		base.Config{HealthCheck: true},
	)
}

// NewConsistentHashringPickerBuilder creates a new picker builder
// that will create consistent hashrings according to the supplied
// config. If the ReplicationFactor is changed, that new parameter
// will be used when the next picker is created.
func NewConsistentHashringPickerBuilder(
	hasher consistent.HasherFunc,
	initialReplicationFactor uint16,
	spread uint8,
) *ConsistentHashringPickerBuilder {
	return &ConsistentHashringPickerBuilder{
		hasher:            hasher,
		replicationFactor: initialReplicationFactor,
		spread:            spread,
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

// ConsistentHashringPickerBuilder is an implementation of base.PickerBuilder and
// is used to build pickers based on updates to the node architecture.
type ConsistentHashringPickerBuilder struct {
	sync.Mutex

	hasher            consistent.HasherFunc
	replicationFactor uint16
	spread            uint8
}

func (b *ConsistentHashringPickerBuilder) MarshalZerologObject(e *zerolog.Event) {
	e.Uint16("consistent-hashring-replication-factor", b.replicationFactor)
	e.Uint8("consistent-hashring-spread", b.spread)
}

func (b *ConsistentHashringPickerBuilder) MustReplicationFactor(rf uint16) {
	if rf == 0 {
		panic("invalid ReplicationFactor")
	}

	b.Lock()
	defer b.Unlock()
	b.replicationFactor = rf
}

func (b *ConsistentHashringPickerBuilder) MustSpread(spread uint8) {
	if spread == 0 {
		panic("invalid Spread")
	}

	b.Lock()
	defer b.Unlock()
	b.spread = spread
}

func (b *ConsistentHashringPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashringPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	b.Lock()
	hashring := consistent.MustNewHashring(b.hasher, b.replicationFactor)
	b.Unlock()

	for sc, scInfo := range info.ReadySCs {
		if err := hashring.Add(subConnMember{
			SubConn: sc,
			key:     scInfo.Address.Addr + scInfo.Address.ServerName,
		}); err != nil {
			return base.NewErrPicker(err)
		}
	}

	if b.spread == 0 {
		return base.NewErrPicker(fmt.Errorf("received invalid spread for consistent hash ring picker builder: %d", b.spread))
	}

	return &consistentHashringPicker{
		hashring: hashring,
		spread:   b.spread,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type consistentHashringPicker struct {
	sync.Mutex
	hashring *consistent.Hashring
	spread   uint8
	rand     *rand.Rand
}

func (p *consistentHashringPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	key := info.Ctx.Value(CtxKey).([]byte)
	members, err := p.hashring.FindN(key, p.spread)
	if err != nil {
		return balancer.PickResult{}, err
	}

	// rand is not safe for concurrent use
	p.Lock()
	index := p.rand.Intn(int(p.spread))
	p.Unlock()

	chosen := members[index].(subConnMember)
	return balancer.PickResult{
		SubConn: chosen.SubConn,
	}, nil
}

var _ base.PickerBuilder = &ConsistentHashringPickerBuilder{}
