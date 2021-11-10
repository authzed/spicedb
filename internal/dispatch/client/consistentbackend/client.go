package consistentbackend

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/jpillora/backoff"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	servok "github.com/authzed/spicedb/internal/proto/servok/api/v1"
	"github.com/authzed/spicedb/pkg/consistent"
)

const (
	errInitializingSmartClient = "unable to initialize smart client: %w"
	errComputingBackend        = "unable to compute backend for request: %w"
	errEstablishingWatch       = "unable to establish watch stream: %w"

	hashringReplicationFactor = 20
)

var protoMarshal = proto.MarshalOptions{Deterministic: true}

// ConsistentBackendClient is a client which utilizes a dynamic source of backends and a consistent
// hashring implementation for consistently calling the same backend for the same request.
type ConsistentBackendClient struct {
	backendsPerKey uint8

	fallbackBackend *backend
	resolverConfig  *EndpointResolverConfig
	endpointConfig  *EndpointConfig

	ringMu sync.Mutex
	ring   *consistent.Hashring
}

// NewConsistentBackendClient creates an instance of the smart client with the specified configuration.
func NewConsistentBackendClient(
	resolverConfig *EndpointResolverConfig,
	endpointConfig *EndpointConfig,
	fallback *FallbackEndpointConfig,
) (*ConsistentBackendClient, error) {
	if resolverConfig.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, resolverConfig.err)
	}
	if endpointConfig.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, endpointConfig.err)
	}
	if fallback.err != nil {
		return nil, fmt.Errorf(errInitializingSmartClient, fallback.err)
	}

	sc := &ConsistentBackendClient{
		1,
		fallback.backend,
		resolverConfig,
		endpointConfig,
		sync.Mutex{},
		consistent.NewHashring(xxhash.Sum64, hashringReplicationFactor),
	}

	return sc, nil
}

func establishServokWatch(ctx context.Context,
	resolverConfig *EndpointResolverConfig,
	endpointConfig *EndpointConfig,
) (servok.EndpointService_WatchClient, error) {

	servokConn, err := grpc.Dial(resolverConfig.endpoint, resolverConfig.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf(errEstablishingWatch, err)
	}

	servokClient := servok.NewEndpointServiceClient(servokConn)

	stream, err := servokClient.Watch(ctx, &servok.WatchRequest{
		RequestTypeOneof: &servok.WatchRequest_Srv{
			Srv: &servok.WatchRequest_SRVRequest{
				Service:  endpointConfig.serviceName,
				Protocol: "tcp",
				DnsName:  endpointConfig.dnsName,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf(errEstablishingWatch, err)
	}

	return stream, nil
}

// Start starts the process which will dynamically update backend membership continually. Stop this
// service by canceling the context.
func (cbc *ConsistentBackendClient) Start(ctx context.Context) {
	b := &backoff.Backoff{
		Jitter: true,
		Max:    1 * time.Minute,
	}

	for ctx.Err() == nil {
		stream, err := establishServokWatch(ctx, cbc.resolverConfig, cbc.endpointConfig)
		if err != nil {
			wait := b.Duration()
			log.Ctx(ctx).Warn().Stringer("retryAfter", wait).Err(err).Msg("unable to establish endpoint resolver connection")
			time.Sleep(wait)

			// We need to re-establish the stream
			continue
		}

		for ctx.Err() == nil {
			endpointResponse, err := stream.Recv()
			if err != nil {
				wait := b.Duration()
				log.Ctx(ctx).Error().Stringer("retryAfter", wait).Err(err).Msg("error reading from endpoint server")

				waitTimer := time.NewTimer(wait)

				// We don't want to wait the whole time if our context gets closed
				select {
				case <-ctx.Done():
				case <-waitTimer.C:
				}

				// We need to re-establish the stream
				break
			}

			b.Reset()
			cbc.updateMembers(ctx, endpointResponse.Endpoints, cbc.endpointConfig.dialOptions)
		}
	}
}

func (cbc *ConsistentBackendClient) updateMembers(ctx context.Context, endpoints []*servok.Endpoint, endpointDialOptions []grpc.DialOption) {
	log.Ctx(ctx).Info().Int("numEndpoints", len(endpoints)).Msg("received servok endpoint update")

	// This is only its own method to get the defer unlock here
	cbc.ringMu.Lock()
	defer cbc.ringMu.Unlock()

	if ctx.Err() != nil {
		log.Ctx(ctx).Fatal().Err(ctx.Err()).Msg("cannot update members, client already stopped")
	}

	membersToRemove := map[string]consistent.Member{}
	for _, existingMember := range cbc.ring.Members() {
		membersToRemove[existingMember.Key()] = existingMember
	}

	for _, endpoint := range endpoints {
		clientEndpoint := fmt.Sprintf("%s:%d", endpoint.Hostname, endpoint.Port)

		conn, err := grpc.Dial(clientEndpoint, endpointDialOptions...)
		if err != nil {
			log.Ctx(ctx).Fatal().Str("endpoint", clientEndpoint).Err(err).Msg("error constructing client for endpoint")
		}

		client := v1.NewDispatchServiceClient(conn)

		memberToAdd := &backend{
			key:    fmt.Sprintf("%d %s", endpoint.Port, endpoint.Hostname),
			client: client,
		}

		memberKey := memberToAdd.Key()

		if _, ok := membersToRemove[memberKey]; ok {
			// This is both an existing and new member, do not remove it
			delete(membersToRemove, memberKey)
		} else {
			// This is a net-new member, add it to the hashring
			log.Ctx(ctx).Debug().Str("memberKey", memberKey).Msg("adding hashring member")
			if err := cbc.ring.Add(memberToAdd); err != nil && !errors.Is(err, consistent.ErrMemberAlreadyExists) {
				log.Ctx(ctx).Fatal().Err(err).Msg("failed to add backend member")
			}
		}
	}

	for memberName, member := range membersToRemove {
		log.Ctx(ctx).Debug().Str("memberName", memberName).Msg("removing hashring member")
		if err := cbc.ring.Remove(member); err != nil && !errors.Is(err, consistent.ErrMemberNotFound) {
			log.Ctx(ctx).Fatal().Err(err).Msg("failed to remove backend member")
		}
	}

	log.Ctx(ctx).Info().Int("numEndpoints", len(endpoints)).Msg("updated smart client endpoint list")
}

func (cbc *ConsistentBackendClient) getConsistentBackend(ctx context.Context, request proto.Message) (*backend, error) {
	requestKey, err := protoMarshal.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf(errComputingBackend, err)
	}

	members, err := cbc.ring.FindN(requestKey, cbc.backendsPerKey)
	if err != nil {
		if err == consistent.ErrNotEnoughMembers && cbc.fallbackBackend != nil {
			log.Ctx(ctx).Warn().
				Str("backend", cbc.fallbackBackend.Key()).
				Str("type", "consistent").
				Msg("using fallback backend")
			return cbc.fallbackBackend, nil
		}

		return nil, fmt.Errorf(errComputingBackend, err)
	}

	chosen := members[rand.Intn(int(cbc.backendsPerKey))].(*backend)

	log.Ctx(ctx).Debug().Str("chosen", chosen.Key()).Msg("chose consistent backend for call")

	return chosen, nil
}

type backend struct {
	key    string
	client v1.DispatchServiceClient
}

// Implements consistent.Member
// This value is what will be hashed for placement on the consistent hash ring.
func (b *backend) Key() string {
	return b.key
}
