package smartclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	client_v0 "github.com/authzed/authzed-go/v0"
	client_v1alpha1 "github.com/authzed/authzed-go/v1alpha1"
	"github.com/cespare/xxhash"
	"github.com/jpillora/backoff"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/consistent"
	servok "github.com/authzed/spicedb/pkg/proto/servok/api/v1"
)

const (
	errInitializingSmartClient = "unable to initialize smart client: %w"
	errComputingBackend        = "unable to compute backend for request: %w"
	errParsingFallbackEndpoint = "unable to parse fallback endpoint: %w"
	errEstablishingWatch       = "unable to establish watch stream: %w"

	hashringReplicationFactor = 20
)

var errNoBackends = errors.New("no backends available for request")

// SmartClient is a client which utilizes a dynamic source of backends and a consistent
// hashring implementation for consistently calling the same backend for the same request.
type SmartClient struct {
	backendsPerKey uint8

	fallbackBackend *backend

	ringMu       sync.Mutex
	ring         *consistent.Hashring
	cancelWatch  context.CancelFunc
	protoMarshal proto.MarshalOptions
}

// NewSmartClient creates an instance of the smart client with the specified configuration.
func NewSmartClient(
	resolverConfig *EndpointResolverConfig,
	endpointConfig *EndpointConfig,
	fallback *FallbackEndpointConfig,
) (*SmartClient, error) {
	if resolverConfig.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, resolverConfig.err)
	}
	if endpointConfig.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, endpointConfig.err)
	}
	if fallback.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, fallback.err)
	}

	servokClientCtx, cancel := context.WithCancel(context.Background())

	sc := &SmartClient{
		1,
		fallback.backend,
		sync.Mutex{},
		consistent.NewHashring(xxhash.Sum64, hashringReplicationFactor),
		cancel,
		proto.MarshalOptions{Deterministic: true},
	}

	go sc.watchAndUpdateMembership(servokClientCtx, resolverConfig, endpointConfig)

	return sc, nil
}

func establishServokWatch(ctx context.Context,
	resolverConfig *EndpointResolverConfig,
	upstreamEndpointDNSName string,
) (servok.EndpointService_WatchClient, error) {

	servokConn, err := grpc.Dial(resolverConfig.endpoint, resolverConfig.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf(errEstablishingWatch, err)
	}

	servokClient := servok.NewEndpointServiceClient(servokConn)

	log.Debug().Str("dnsName", upstreamEndpointDNSName).Msg("")

	stream, err := servokClient.Watch(ctx, &servok.WatchRequest{
		DnsName: upstreamEndpointDNSName,
	})
	if err != nil {
		return nil, fmt.Errorf(errEstablishingWatch, err)
	}

	return stream, nil
}

func (sc *SmartClient) watchAndUpdateMembership(
	ctx context.Context,
	resolverConfig *EndpointResolverConfig,
	endpointConfig *EndpointConfig,
) {
	b := &backoff.Backoff{
		Jitter: true,
	}

	for ctx.Err() == nil {
		stream, err := establishServokWatch(ctx, resolverConfig, endpointConfig.dnsName)
		if err != nil {
			wait := b.Duration()
			log.Warn().Stringer("retryAfter", wait).Err(err).Msg("unable to establish endpoint resolver connection")
			time.Sleep(wait)

			// We need to re-establish the stream
			continue
		}

		for ctx.Err() == nil {
			endpointResponse, err := stream.Recv()
			if err != nil {
				wait := b.Duration()
				log.Error().Stringer("retryAfter", wait).Err(err).Msg("error reading from endpoint server")
				time.Sleep(wait)

				// We need to re-establish the stream
				break
			} else {
				b.Reset()
				sc.updateMembers(ctx, endpointResponse.Endpoints, endpointConfig.dialOptions)
			}
		}
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
			key:             fmt.Sprintf("%d %s", endpoint.Port, endpoint.Hostname),
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
		if err == consistent.ErrNotEnoughMembers && sc.fallbackBackend != nil {
			log.Warn().Str("backend", sc.fallbackBackend.Key()).Msg("using fallback backend")
			return sc.fallbackBackend, nil
		}

		return nil, fmt.Errorf(errComputingBackend, err)
	}

	chosen := members[rand.Intn(int(sc.backendsPerKey))].(*backend)

	log.Debug().Str("chosen", chosen.Key()).Msg("chose consistent backend for call")

	return chosen, nil
}

func (sc *SmartClient) getRandomBackend() (*backend, error) {
	allMembers := sc.ring.Members()
	if len(allMembers) < 1 {
		if sc.fallbackBackend != nil {
			log.Warn().Str("backend", sc.fallbackBackend.Key()).Msg("using fallback backend")
			return sc.fallbackBackend, nil
		}
		return nil, errNoBackends

	}
	return allMembers[rand.Intn(len(allMembers))].(*backend), nil
}

type backend struct {
	key             string
	client_v0       *client_v0.Client
	client_v1alpha1 *client_v1alpha1.Client
}

// Implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (b *backend) Key() string {
	return b.key
}
