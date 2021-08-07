package smartclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	client_v0 "github.com/authzed/authzed-go/v0"
	client_v1alpha1 "github.com/authzed/authzed-go/v1alpha1"
	"github.com/cespare/xxhash"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/consistent"
	servok "github.com/authzed/spicedb/pkg/proto/servok/api/v1"
	"github.com/authzed/spicedb/pkg/x509util"
)

const (
	errInitializingSmartClient = "unable to initialize smart client: %w"
	errComputingBackend        = "unable to marshal request: %w"
)

var errNoBackends = errors.New("no backends available for request")

// SmartClient is a client which utilizes a dynamic source of backends and a consistent
// hashring implementation for consistently calling the same backend for the same request.
type SmartClient struct {
	backendsPerKey uint8

	ringMu       sync.Mutex
	ring         *consistent.Hashring
	cancelWatch  context.CancelFunc
	protoMarshal proto.MarshalOptions
}

// NewSmartClient creates an instance of the smart client with the specified configuration.
func NewSmartClient(
	servokEndpoint string,
	servokCAPath string,
	endpointDNSName string,
	endpointCAPath string,
	endpointToken string,
) (*SmartClient, error) {
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
		consistent.NewHashring(xxhash.Sum64, 20),
		cancel,
		proto.MarshalOptions{Deterministic: true},
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

	membersToRemove := map[string]consistent.Member{}
	for _, existingMember := range sc.ring.Members() {
		membersToRemove[existingMember.Key()] = existingMember
	}

	for _, endpoint := range endpoints {
		clientEndpoint := fmt.Sprintf("%s:%d", endpoint.Hostname, endpoint.Port)

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

		memberKey := memberToAdd.Key()

		if _, ok := membersToRemove[memberKey]; ok {
			// This is both an existing and new member, do not remove it
			delete(membersToRemove, memberKey)
		} else {
			// This is a net-new member, add it to the hashring
			log.Debug().Str("memberKey", memberKey).Msg("adding hashring member")
			sc.ring.Add(memberToAdd)
		}
	}

	for memberName, member := range membersToRemove {
		log.Debug().Str("memberName", memberName).Msg("removing hashring member")
		sc.ring.Remove(member)
	}

	log.Info().Int("numEndpoints", len(endpoints)).Msg("updated smart client endpoint list")
}

// Stop will cancel the client watch and clean up the pool
func (sc *SmartClient) Stop() {
	sc.ringMu.Lock()
	defer sc.ringMu.Unlock()

	sc.cancelWatch()

	for _, member := range sc.ring.Members() {
		sc.ring.Remove(member)
	}
}

func (sc *SmartClient) getConsistentBackend(request proto.Message) (*backend, error) {
	requestKey, err := sc.protoMarshal.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf(errComputingBackend, err)
	}

	members, err := sc.ring.FindN(requestKey, sc.backendsPerKey)
	if err != nil {
		return nil, fmt.Errorf(errComputingBackend, err)
	}

	chosen := members[rand.Intn(int(sc.backendsPerKey))].(*backend)

	log.Debug().Str("chosen", chosen.Key()).Msg("chose consistent backend for call")

	return chosen, nil
}

func (sc *SmartClient) getRandomBackend() (*backend, error) {
	allMembers := sc.ring.Members()
	if len(allMembers) < 1 {
		return nil, errNoBackends
	}
	return allMembers[rand.Intn(len(allMembers))].(*backend), nil
}

type backend struct {
	endpoint        *servok.Endpoint
	client_v0       *client_v0.Client
	client_v1alpha1 *client_v1alpha1.Client
}

// Implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (b *backend) Key() string {
	return fmt.Sprintf("%d %s", b.endpoint.Port, b.endpoint.Hostname)
}
