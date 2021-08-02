package smartclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"

	client_v0 "github.com/authzed/authzed-go/v0"
	client_v1alpha1 "github.com/authzed/authzed-go/v1alpha1"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	servok "github.com/authzed/spicedb/pkg/proto/servok/api/v1"
	"github.com/authzed/spicedb/pkg/x509util"
)

var binarySeparator = []byte{':'}

const errInitializingSmartClient = "unable to initialize smart client: %w"

type SmartClient struct {
	backendsPerKey int

	ringMu      sync.Mutex
	ring        *consistent.Consistent
	cancelWatch context.CancelFunc
}

type HasherFunc func([]byte) uint64

func (f HasherFunc) Sum64(data []byte) uint64 {
	return f(data)
}

func NewSmartClient(
	servokEndpoint string,
	servokCAPath string,
	endpointDNSName string,
	endpointCAPath string,
	endpointToken string,
) (*SmartClient, error) {

	cfg := consistent.Config{
		PartitionCount:    7919,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            HasherFunc(xxhash.Sum64),
	}

	pool, err := x509util.CustomCertPool(servokCAPath)
	if err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, err)
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool})

	servokConn, err := grpc.Dial(
		servokEndpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	)
	if err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, err)
	}

	servokClient := servok.NewEndpointServiceClient(servokConn)

	servokClientCtx, cancel := context.WithCancel(context.Background())

	sc := &SmartClient{
		1,
		sync.Mutex{},
		consistent.New(nil, cfg),
		cancel,
	}

	stream, err := servokClient.Watch(servokClientCtx, &servok.WatchRequest{
		DnsName: endpointDNSName,
	})
	if err != nil {
		cancel()

		return nil, fmt.Errorf(errInitializingSmartClient, err)
	}

	endpointPool, err := x509util.CustomCertPool(endpointCAPath)
	if err != nil {
		cancel()

		return nil, fmt.Errorf(errInitializingSmartClient, err)
	}
	spicedbCreds := credentials.NewTLS(&tls.Config{RootCAs: endpointPool, ServerName: endpointDNSName})

	endpointClientDialOptions := []grpc.DialOption{
		client_v0.Token(endpointToken),
		grpc.WithTransportCredentials(spicedbCreds),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}

	go sc.watchAndUpdateMembership(servokClientCtx, stream, endpointClientDialOptions...)

	return sc, nil
}

func (sc *SmartClient) watchAndUpdateMembership(
	ctx context.Context,
	stream servok.EndpointService_WatchClient,
	endpointDialOptions ...grpc.DialOption,
) {
	for ctx.Err() == nil {
		endpointResponse, err := stream.Recv()
		if err != nil {
			log.Fatal().Err(err).Msg("error reading from endpoint server")
		}

		sc.updateMembers(ctx, endpointResponse.Endpoints, endpointDialOptions)
	}
}

func (sc *SmartClient) updateMembers(ctx context.Context, endpoints []*servok.Endpoint, endpointDialOptions []grpc.DialOption) {
	log.Info().Int("numEndpoints", len(endpoints)).Msg("received servok endpoint update")

	// This is only its own method to get the defer unlock here
	sc.ringMu.Lock()
	defer sc.ringMu.Unlock()

	if ctx.Err() != nil {
		log.Fatal().Err(ctx.Err()).Msg("cannot update members, client already stopped")
	}

	membersToRemove := map[string]struct{}{}
	for _, existingMember := range sc.ring.GetMembers() {
		membersToRemove[existingMember.String()] = struct{}{}
	}

	for _, endpoint := range endpoints {
		clientEndpoint := fmt.Sprintf("%s:%d", endpoint.Hostname, endpoint.Port)

		log.Debug().Str("endpoint", clientEndpoint).Msg("constructing client for endpoint")

		v0clientForMember, err := client_v0.NewClient(clientEndpoint, endpointDialOptions...)
		if err != nil {
			log.Fatal().Str("endpoint", clientEndpoint).Err(err).Msg("error constructing client for endpoint")
		}

		v1alpha1clientForMember, err := client_v1alpha1.NewClient(clientEndpoint, endpointDialOptions...)
		if err != nil {
			log.Fatal().Str("endpoint", clientEndpoint).Err(err).Msg("error constructing client for endpoint")
		}

		memberToAdd := &backend{
			endpoint:        endpoint,
			client_v0:       v0clientForMember,
			client_v1alpha1: v1alpha1clientForMember,
		}

		log.Debug().Stringer("memberName", memberToAdd).Msg("adding hashring member")
		sc.ring.Add(memberToAdd)

		delete(membersToRemove, memberToAdd.String())
	}

	for memberName := range membersToRemove {
		log.Debug().Str("memberName", memberName).Msg("removing hashring member")
		sc.ring.Remove(memberName)
	}

	log.Info().Int("numEndpoints", len(endpoints)).Msg("updated smart client endpoint list")
}

// Stop will cancel the client watch and clean up the pool
func (sc *SmartClient) Stop() {
	sc.ringMu.Lock()
	defer sc.ringMu.Unlock()

	sc.cancelWatch()

	for _, member := range sc.ring.GetMembers() {
		sc.ring.Remove(member.String())
	}
}

func (sc *SmartClient) getConsistentBackend(requestKey []byte) (*backend, error) {
	members, err := sc.ring.GetClosestN(requestKey, sc.backendsPerKey)
	if err != nil {
		return nil, err
	}

	chosen := members[rand.Intn(sc.backendsPerKey)].(*backend)

	log.Debug().Stringer("chosen", chosen).Msg("chose consistent backend for call")

	return chosen, nil
}

func (sc *SmartClient) getRandomBackend() *backend {
	allMembers := sc.ring.GetMembers()
	return allMembers[rand.Intn(len(allMembers))].(*backend)
}

type backend struct {
	endpoint        *servok.Endpoint
	client_v0       *client_v0.Client
	client_v1alpha1 *client_v1alpha1.Client
}

// Implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (b *backend) String() string {
	return fmt.Sprintf("0 %d %d %s", b.endpoint.Weight, b.endpoint.Port, b.endpoint.Hostname)
}
