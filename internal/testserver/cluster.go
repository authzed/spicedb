package testserver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"

	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	hashbalancer "github.com/authzed/spicedb/pkg/balancer"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/secrets"
)

const TestResolverScheme = "test"

type TempError struct{}

func (t TempError) Error() string {
	return "no dialers yet"
}

func (t TempError) Temporary() bool {
	return true
}

type dialerFunc func(ctx context.Context, s string) (net.Conn, error)

// track prefixes used for making test clusters to avoid registering the same
// prometheus subsystem twice in one run
var usedPrefixes sync.Map

func getPrefix(t testing.TB) string {
	for {
		prefix, err := secrets.TokenHex(8)
		require.NoError(t, err)
		if _, ok := usedPrefixes.Load(prefix); !ok {
			usedPrefixes.Store(prefix, struct{}{})
			return prefix
		}
	}
}

var testResolverBuilder = &SafeManualResolverBuilder{}

func init() {
	// register hashring balancer
	balancer.Register(hashbalancer.NewConsistentHashringBuilder(xxhash.Sum64, 20, 1))

	// Register a manual resolver.Builder  that we can feed addresses for tests
	// Registration is not thread safe, so we register a single resolver.Builder
	// to handle all clusters, rather than registering a unique resolver.Builder
	// per cluster.
	resolver.Register(testResolverBuilder)
}

// SafeManualResolverBuilder is a resolver builder that builds SafeManualResolvers
// it is similar to manual.Resolver in grpc, but is thread safe
type SafeManualResolverBuilder struct {
	resolvers sync.Map
	addrs     sync.Map
}

func (b *SafeManualResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if target.URL.Scheme != TestResolverScheme {
		return nil, fmt.Errorf("test resolver builder only works with test:// addresses")
	}
	var addrs []resolver.Address
	addrVal, ok := b.addrs.Load(target.URL.Hostname())
	if !ok {
		addrs = make([]resolver.Address, 0)
	} else {
		addrs = addrVal.([]resolver.Address)
	}
	r := &SafeManualResolver{
		prefix: target.URL.Hostname(),
		cc:     cc,
		opts:   opts,
		addrs:  addrs,
	}
	b.resolvers.Store(target.URL.Hostname(), r)
	return r, nil
}

func (b *SafeManualResolverBuilder) Scheme() string {
	return "test"
}

func (b *SafeManualResolverBuilder) SetAddrs(prefix string, addrs []resolver.Address) {
	b.addrs.Store(prefix, addrs)
}

func (b *SafeManualResolverBuilder) ResolveNow(prefix string) {
	r, ok := b.resolvers.Load(prefix)
	if !ok {
		fmt.Println("NO RESOLVER YET") // shouldn't happen, but log
		return
	}
	r.(*SafeManualResolver).ResolveNow(resolver.ResolveNowOptions{})
}

// SafeManualResolver is the resolver type that SafeManualResolverBuilder builds
// it returns a static list of addresses
type SafeManualResolver struct {
	prefix string
	cc     resolver.ClientConn
	opts   resolver.BuildOptions
	addrs  []resolver.Address
}

// ResolveNow implements the resolver.Resolver interface
// It sends the static list of addresses to the underlying resolver.ClientConn
func (r *SafeManualResolver) ResolveNow(options resolver.ResolveNowOptions) {
	if r.cc == nil {
		return
	}
	if err := r.cc.UpdateState(resolver.State{Addresses: r.addrs}); err != nil {
		fmt.Println("ERROR UPDATING STATE", err) // shouldn't happen, log
	}
}

// Close implements the resolver.Resolver interface
func (r *SafeManualResolver) Close() {}

// TestClusterWithDispatch creates a cluster with `size` nodes
// The cluster has a real dispatch stack that uses bufconn grpc connections
func TestClusterWithDispatch(t testing.TB, size uint, ds datastore.Datastore) ([]*grpc.ClientConn, func()) {
	// each cluster gets a unique prefix since grpc resolution is process-global
	prefix := getPrefix(t)

	// make placeholder resolved addresses, 1 per node
	addresses := make([]resolver.Address, 0, size)
	for i := uint(0); i < size; i++ {
		addresses = append(addresses, resolver.Address{
			Addr:       fmt.Sprintf("%s_%d", prefix, i),
			ServerName: "",
		})
	}
	testResolverBuilder.SetAddrs(prefix, addresses)

	dialers := make([]dialerFunc, 0, size)
	conns := make([]*grpc.ClientConn, 0, size)
	cancelFuncs := make([]func(), 0, size)

	for i := uint(0); i < size; i++ {
		dispatcher, err := combineddispatch.NewDispatcher(
			combineddispatch.UpstreamAddr("test://"+prefix),
			combineddispatch.PrometheusSubsystem(fmt.Sprintf("%s_%d_client_dispatch", prefix, i)),
			combineddispatch.GrpcDialOpts(
				grpc.WithDefaultServiceConfig(hashbalancer.BalancerServiceConfig),
				grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
					// it's possible grpc tries to dial before we have set the
					// buffconn dialers, we have to return a "TempError" so that
					// grpc knows to retry the connection.
					if len(dialers) == 0 {
						return nil, TempError{}
					}
					// "s" here will be the address from the manual resolver
					// like `<prefix>_<node number>`
					i, err := strconv.Atoi(strings.TrimPrefix(s, prefix+"_"))
					require.NoError(t, err)
					return dialers[i](ctx, s)
				}),
			),
		)
		require.NoError(t, err)

		srv, err := server.NewConfigWithOptions(
			server.WithDatastore(ds),
			server.WithDispatcher(dispatcher),
			server.WithDispatchMaxDepth(50),
			server.WithGRPCServer(util.GRPCServerConfig{
				Network: util.BufferedNetwork,
				Enabled: true,
			}),
			server.WithSchemaPrefixesRequired(false),
			server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
				return ctx, nil
			}),
			server.WithHTTPGateway(util.HTTPServerConfig{Enabled: false}),
			server.WithDashboardAPI(util.HTTPServerConfig{Enabled: false}),
			server.WithMetricsAPI(util.HTTPServerConfig{Enabled: false}),
			server.WithDispatchServer(util.GRPCServerConfig{
				Enabled: true,
				Network: util.BufferedNetwork,
			}),
			server.WithDispatchClusterMetricsPrefix(fmt.Sprintf("%s_%d_dispatch", prefix, i)),
		).Complete()
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			require.NoError(t, srv.Run(ctx))
		}()
		cancelFuncs = append(cancelFuncs, cancel)

		dialers = append(dialers, srv.DispatchNetDialContext)
		conn, err := srv.GRPCDialContext(ctx, grpc.WithBlock())
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	// resolve after dialers have been set to initialize connections
	testResolverBuilder.ResolveNow(prefix)

	return conns, func() {
		for _, c := range conns {
			require.NoError(t, c.Close())
		}
		for _, c := range cancelFuncs {
			c()
		}
	}
}
