package balancer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"

	"github.com/authzed/spicedb/pkg/consistent"
)

type ctxKey string

const (
	// name is the name of consistent-hashring balancer.
	name = "consistent-hashring"

	// serviceConfig is a service config that sets the default balancer
	// to the consistent-hashring balancer
	serviceConfig = `{"loadBalancingPolicy":"%s"}`

	// CtxKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	CtxKey ctxKey = "requestKey"
)

var logger = grpclog.Component("consistenthashring")

// NewConsistentHashringBuilder creates a new balancer.Builder that
// will create a consistent hashring balancer with the given config.
// Before making a connection, register it with grpc with:
// `balancer.Register(consistent.NewConsistentHashringBuilder(hasher, factor, spread))`
func NewConsistentHashringBuilder(hasher consistent.HasherFunc, replicationFactor uint16, spread uint8) balancer.Builder {
	return base.NewBalancerBuilder(
		NameForReplicationFactor(replicationFactor),
		&consistentHashringPickerBuilder{hasher: hasher, replicationFactor: replicationFactor, spread: spread},
		base.Config{HealthCheck: true},
	)
}

// NameForReplicationFactor returns the name of the balancer for a given replication factor
func NameForReplicationFactor(replicationFactor uint16) string {
	return fmt.Sprintf(name+"-rf-%d", replicationFactor)
}

// ServiceConfigForBalancerName provides the gRPC service configuration string for
// a hashring balancer with a specific replication factor by its name
func ServiceConfigForBalancerName(balancerName string) string {
	return fmt.Sprintf(serviceConfig, balancerName)
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

type consistentHashringPickerBuilder struct {
	hasher            consistent.HasherFunc
	replicationFactor uint16
	spread            uint8
}

func (b *consistentHashringPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashringPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	hashring := consistent.MustNewHashring(b.hasher, b.replicationFactor)
	for sc, scInfo := range info.ReadySCs {
		if err := hashring.Add(subConnMember{
			SubConn: sc,
			key:     scInfo.Address.Addr + scInfo.Address.ServerName,
		}); err != nil {
			return base.NewErrPicker(err)
		}
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
