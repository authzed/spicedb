package singleflight

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const defaultFalsePositiveRate = 0.01

var (
	singleFlightCount       = promauto.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	singleFlightCountConfig = prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "dispatch",
		Name:      "single_flight_total",
		Help:      "total number of dispatch requests that were single flighted",
	}
)

func New(delegate dispatch.Dispatcher, handler keys.Handler) dispatch.Dispatcher {
	return &Dispatcher{
		delegate:   delegate,
		keyHandler: handler,
	}
}

type Dispatcher struct {
	delegate   dispatch.Dispatcher
	keyHandler keys.Handler

	checkGroup  singleflight.Group[string, *v1.DispatchCheckResponse]
	expandGroup singleflight.Group[string, *v1.DispatchExpandResponse]
}

func (d *Dispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	key, err := d.keyHandler.CheckDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchCheck error")
	}

	keyString := hex.EncodeToString(key)

	// Check if the key has already been part of a dispatch. If so, this represents a
	// likely recursive call, so we dispatch it to the delegate to avoid the singleflight from blocking it.
	// If the bloom filter presents a false positive, a dispatch will happen, which is a small inefficiency
	// traded-off to prevent a recursive-call deadlock
	serializedBloom, err := validateTraversalRecursion(keyString, req.Metadata)
	if errors.Is(err, ErrLoopDetect) {
		singleFlightCount.WithLabelValues("DispatchCheck", "loop").Inc()
		return d.delegate.DispatchCheck(ctx, req)
	}

	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}

	req.Metadata.TraversalBloom = serializedBloom

	v, isShared, err := d.checkGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchCheckResponse, error) {
		return d.delegate.DispatchCheck(innerCtx, req)
	})

	singleFlightCount.WithLabelValues("DispatchCheck", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}

	return v, err
}

func (d *Dispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	key, err := d.keyHandler.ExpandDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchExpand error")
	}

	keyString := hex.EncodeToString(key)
	serializedBloom, err := validateTraversalRecursion(keyString, req.Metadata)
	if errors.Is(err, ErrLoopDetect) {
		singleFlightCount.WithLabelValues("DispatchExpand", "loop").Inc()
		return d.delegate.DispatchExpand(ctx, req)
	}

	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}

	req.Metadata.TraversalBloom = serializedBloom

	v, isShared, err := d.expandGroup.Do(ctx, keyString, func(ictx context.Context) (*v1.DispatchExpandResponse, error) {
		return d.delegate.DispatchExpand(ictx, req)
	})

	singleFlightCount.WithLabelValues("DispatchExpand", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}
	return v, err
}

var ErrLoopDetect = errors.New("traversal recursion loop detected")

// validateTraversalRecursion determines from the ResolverMeta.TraversalBloom value if a traversal loop has happened
// based on the argument key. As a bloom filter, this may lead to false positives,
// so clients should take that into account.
func validateTraversalRecursion(key string, meta *v1.ResolverMeta) ([]byte, error) {
	if len(meta.TraversalBloom) == 0 {
		return nil, status.Error(codes.Internal, fmt.Errorf("required traversal bloom filter is missing").Error())
	}

	bf := &bloom.BloomFilter{}
	if err := bf.UnmarshalBinary(meta.TraversalBloom); err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("unable to unmarshall traversal bloom filter: %w", err).Error())
	}

	if bf.TestString(key) {
		return nil, ErrLoopDetect
	}

	bf = bf.AddString(key)
	modifiedBloomFilter, err := bf.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return modifiedBloomFilter, nil
}

func MustNewTraversalBloomFilter(depth uint) []byte {
	bf := bloom.NewWithEstimates(depth, defaultFalsePositiveRate)

	modifiedBloomFilter, err := bf.MarshalBinary()
	if err != nil {
		panic("unexpected error while serializing empty bloom filter")
	}

	return modifiedBloomFilter
}

func (d *Dispatcher) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	return d.delegate.DispatchReachableResources(req, stream)
}

func (d *Dispatcher) DispatchLookupResources(req *v1.DispatchLookupResourcesRequest, stream dispatch.LookupResourcesStream) error {
	return d.delegate.DispatchLookupResources(req, stream)
}

func (d *Dispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return d.delegate.DispatchLookupSubjects(req, stream)
}

func (d *Dispatcher) Close() error                    { return d.delegate.Close() }
func (d *Dispatcher) ReadyState() dispatch.ReadyState { return d.delegate.ReadyState() }
