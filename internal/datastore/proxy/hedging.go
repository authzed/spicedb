package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/tdigest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
	datastore.Datastore

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
) (datastore.Datastore, error) {
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
) (datastore.Datastore, error) {
	if initialSlowRequestThreshold < 0 {
		return nil, fmt.Errorf("initial slow request threshold negative")
	}

	if maxSampleCount < minMaxRequestsThreshold {
		return nil, fmt.Errorf("maxSampleCount must be >=%d", minMaxRequestsThreshold)
	}

	if hedgingQuantile <= 0.0 || hedgingQuantile >= 1.0 {
		return nil, fmt.Errorf("hedgingQuantile must be in the range (0.0-1.0) exclusive")
	}

	return hedgingProxy{
		delegate,
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(timeSource, initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
	}, nil
}

func (hp hedgingProxy) Unwrap() datastore.Datastore {
	return hp.Datastore
}

func (hp hedgingProxy) OptimizedRevision(ctx context.Context) (rev datastore.Revision, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		delegatedRev, delegatedErr := hp.Datastore.OptimizedRevision(ctx)
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
		delegatedRev, delegatedErr := hp.Datastore.HeadRevision(ctx)
		once.Do(func() {
			rev = delegatedRev
			err = delegatedErr
		})
		responseReady <- struct{}{}
	}

	hp.headRevisionHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegate := hp.Datastore.SnapshotReader(rev)
	return &hedgingReader{delegate, hp}
}

type hedgingReader struct {
	datastore.Reader

	p hedgingProxy
}

func (hp hedgingReader) ReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (ns *core.NamespaceDefinition, createdAt datastore.Revision, err error) {
	var once sync.Once
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		delegatedNs, delegatedRev, delegatedErr := hp.Reader.ReadNamespaceByName(ctx, nsName)
		once.Do(func() {
			ns = delegatedNs
			createdAt = delegatedRev
			err = delegatedErr
		})
		responseReady <- struct{}{}
	}

	hp.p.readNamespaceHedger(ctx, subreq)

	return
}

func (hp hedgingReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	return hp.executeQuery(ctx, func(c context.Context) (datastore.RelationshipIterator, error) {
		return hp.Reader.QueryRelationships(ctx, filter, options...)
	})
}

func (hp hedgingReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	return hp.executeQuery(ctx, func(c context.Context) (datastore.RelationshipIterator, error) {
		return hp.Reader.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
	})
}

func (hp hedgingReader) executeQuery(
	ctx context.Context,
	exec func(context.Context) (datastore.RelationshipIterator, error),
) (delegateIterator datastore.RelationshipIterator, err error) {
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
			for range tempIterator {
				break
			}
		}
		responseReady <- struct{}{}
	}

	hp.p.queryTuplesHedger(ctx, subreq)

	return
}
