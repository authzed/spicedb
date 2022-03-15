package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/benbjohnson/clock"
	"github.com/influxdata/tdigest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

var hedgeableCount = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "hedgeable_requests_total",
	Help:      "total number of datastore requests which are eligible for hedging",
})

var hedgedCount = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "hedged_requests_total",
	Help:      "total number of requests which have been hedged",
})

const (
	minMaxRequestsThreshold   = 1000
	defaultTDigestCompression = float64(1000)
)

type subrequest func(ctx context.Context, responseReady chan<- struct{})

type hedger func(ctx context.Context, req subrequest)

func newHedger(
	timeSource clock.Clock,
	initialSlowRequestThreshold time.Duration,
	maxSampleCount uint64,
	quantile float64,
) hedger {
	var digestLock sync.Mutex

	digests := []*tdigest.TDigest{
		tdigest.NewWithCompression(defaultTDigestCompression),
		tdigest.NewWithCompression(defaultTDigestCompression),
	}

	// We pre-load the first digest with the initial slow request threshold at a weight
	// such that we have reasonable data for our first request and so the other digest
	// will be out of phase with this one, meaning when the first digest gets to
	// maxSampleCount, the other digest will already be 50% warmed up.
	digests[0].Add(initialSlowRequestThreshold.Seconds(), float64(maxSampleCount)/2)

	return func(ctx context.Context, req subrequest) {
		responseReady := make(chan struct{}, 1)

		digestLock.Lock()
		slowRequestThresholdSeconds := digests[0].Quantile(quantile)
		digestLock.Unlock()
		slowRequestThreshold := time.Duration(slowRequestThresholdSeconds * float64(time.Second))

		timer := timeSource.Timer(slowRequestThreshold)
		originalStart := timeSource.Now()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		hedgeableCount.Inc()
		go req(ctx, responseReady)

		var duration time.Duration

		select {
		case <-responseReady:
			duration = timeSource.Since(originalStart)
		case <-timer.C:
			log.Ctx(ctx).Debug().Dur("after", slowRequestThreshold).Msg("sending hedged datastore request")
			hedgedCount.Inc()

			hedgedResponseReady := make(chan struct{}, 1)
			hedgedStart := timeSource.Now()
			go req(ctx, hedgedResponseReady)

			select {
			case <-responseReady:
				duration = timeSource.Since(originalStart)
			case <-hedgedResponseReady:
				duration = timeSource.Since(hedgedStart)
			}
		}

		digestLock.Lock()
		defer digestLock.Unlock()

		// Swap the current active digest if it has too many samples
		if digests[0].Count() >= float64(maxSampleCount) {
			log.Ctx(ctx).Trace().Float64("count", digests[0].Count()).Msg("switching to next hedging digest")
			exhausted := digests[0]
			digests = digests[1:]
			exhausted.Reset()
			digests = append(digests, exhausted)
		}

		// Record the duration to all candidate digests
		log.Ctx(ctx).Trace().Dur("duration", duration).Msg("adding sample duration to statistics")
		durSeconds := duration.Seconds()
		for _, digest := range digests {
			digest.Add(durSeconds, 1)
		}
	}
}

type hedgingProxy struct {
	delegate datastore.Datastore

	revisionHedger      hedger
	headRevisionHedger  hedger
	readNamespaceHedger hedger
	queryTuplesHedger   hedger
}

// NewHedgingProxy creates a proxy which performs request hedging on read operations
// according to the specified config.
func NewHedgingProxy(
	delegate datastore.Datastore,
	initialSlowRequestThreshold time.Duration,
	maxSampleCount uint64,
	hedgingQuantile float64,
) datastore.Datastore {
	return newHedgingProxyWithTimeSource(
		delegate,
		initialSlowRequestThreshold,
		maxSampleCount,
		hedgingQuantile,
		clock.New(),
	)
}

func newHedgingProxyWithTimeSource(
	delegate datastore.Datastore,
	initialSlowRequestThreshold time.Duration,
	maxSampleCount uint64,
	hedgingQuantile float64,
	timeSource clock.Clock,
) datastore.Datastore {
	if initialSlowRequestThreshold < 0 {
		panic("initial slow request threshold negative")
	}

	if maxSampleCount < minMaxRequestsThreshold {
		panic(fmt.Sprintf("maxSampleCount must be >=%d", minMaxRequestsThreshold))
	}

	if hedgingQuantile <= 0.0 || hedgingQuantile >= 1.0 {
		panic("hedingQuantile must be in the range (0.0-1.0) exclusive")
	}

	return hedgingProxy{
		delegate,
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
	}
}

func (hp hedgingProxy) OptimizedRevision(ctx context.Context) (rev datastore.Revision, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		delegatedRev, delegatedErr := hp.delegate.OptimizedRevision(ctx)
		once.Do(func() {
			rev = delegatedRev
			err = delegatedErr
		})
		responseReady <- struct{}{}
	}

	hp.revisionHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) HeadRevision(ctx context.Context) (rev datastore.Revision, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		delegatedRev, delegatedErr := hp.delegate.HeadRevision(ctx)
		once.Do(func() {
			rev = delegatedRev
			err = delegatedErr
		})
		responseReady <- struct{}{}
	}

	hp.headRevisionHedger(ctx, subreq)

	return
}

// SeedRevision initializes the first transaction revision.
func (hp hedgingProxy) SeedRevision(ctx context.Context) (datastore.Revision, error) {
	return datastore.NoRevision, nil
}

func (hp hedgingProxy) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (ns *v0.NamespaceDefinition, createdAt datastore.Revision, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		delegatedNs, delegatedRev, delegatedErr := hp.delegate.ReadNamespace(ctx, nsName, revision)
		once.Do(func() {
			ns = delegatedNs
			createdAt = delegatedRev
			err = delegatedErr
		})
		responseReady <- struct{}{}
	}

	hp.readNamespaceHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) Close() error {
	return hp.delegate.Close()
}

func (hp hedgingProxy) IsReady(ctx context.Context) (bool, error) {
	return hp.delegate.IsReady(ctx)
}

func (hp hedgingProxy) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	return hp.delegate.DeleteRelationships(ctx, preconditions, filter)
}

func (hp hedgingProxy) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, updates []*v1.RelationshipUpdate) (datastore.Revision, error) {
	return hp.delegate.WriteTuples(ctx, preconditions, updates)
}

func (hp hedgingProxy) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return hp.delegate.Watch(ctx, afterRevision)
}

func (hp hedgingProxy) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	return hp.delegate.WriteNamespace(ctx, newConfig)
}

func (hp hedgingProxy) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return hp.delegate.DeleteNamespace(ctx, nsName)
}

func (hp hedgingProxy) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	options ...options.QueryOptionsOption,
) (iter datastore.TupleIterator, err error) {
	return hp.executeQuery(ctx, func(c context.Context) (datastore.TupleIterator, error) {
		return hp.delegate.QueryTuples(ctx, filter, revision, options...)
	})
}

func (hp hedgingProxy) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.TupleIterator, err error) {
	return hp.executeQuery(ctx, func(c context.Context) (datastore.TupleIterator, error) {
		return hp.delegate.ReverseQueryTuples(ctx, subjectFilter, revision, opts...)
	})
}

func (hp hedgingProxy) executeQuery(
	ctx context.Context,
	exec func(context.Context) (datastore.TupleIterator, error),
) (delegateIterator datastore.TupleIterator, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		tempIterator, tempErr := exec(ctx)
		resultsUsed := false
		once.Do(func() {
			delegateIterator = tempIterator
			err = tempErr
			resultsUsed = true
		})
		// close the unused iterator
		// only the first call to once.Do will run the function, so whichever
		// hedged request is slower will have resultsUsed = false
		if !resultsUsed && tempErr == nil {
			tempIterator.Close()
		}
		responseReady <- struct{}{}
	}

	hp.queryTuplesHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return hp.delegate.CheckRevision(ctx, revision)
}

func (hp hedgingProxy) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*v0.NamespaceDefinition, error) {
	return hp.delegate.ListNamespaces(ctx, revision)
}
