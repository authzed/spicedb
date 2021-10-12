package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/influxdata/tdigest"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	minMaxRequestsThreshold   = 1000
	defaultTDigestCompression = float64(1000)
)

type subrequest func(ctx context.Context, responseReady chan<- struct{})

type hedger func(ctx context.Context, req subrequest)

func newHedger(
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
		responseReady := make(chan struct{})

		digestLock.Lock()
		slowRequestThresholdSeconds := digests[0].Quantile(quantile)
		digestLock.Unlock()
		slowRequestThreshold := time.Duration(slowRequestThresholdSeconds * float64(time.Second))

		timer := time.NewTimer(slowRequestThreshold)
		start := time.Now()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go req(ctx, responseReady)

		select {
		case <-responseReady:
		case <-timer.C:
			log.Debug().Dur("after", slowRequestThreshold).Msg("sending hedged datastore request")
			go req(ctx, responseReady)
			<-responseReady
		}

		// Compute how long it took for us to get any answer
		duration := time.Since(start)
		digestLock.Lock()
		defer digestLock.Unlock()

		// Swap the current active digest if it has too many samples
		if digests[0].Count() >= float64(maxSampleCount) {
			log.Trace().Float64("count", digests[0].Count()).Msg("switching to next hedging digest")
			exhausted := digests[0]
			digests = digests[1:]
			exhausted.Reset()
			digests = append(digests, exhausted)
		}

		// Record the duration to all candidate digests
		log.Trace().Dur("duration", duration).Msg("adding sample duration to statistics")
		durSeconds := duration.Seconds()
		for _, digest := range digests {
			digest.Add(durSeconds, 1)
		}
	}
}

type hedgingProxy struct {
	delegate datastore.Datastore

	revisionHedger      hedger
	syncRevisionHedger  hedger
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
	if initialSlowRequestThreshold < 0 {
		panic("initial slow request threshold negative")
	}

	if maxSampleCount < minMaxRequestsThreshold {
		panic(fmt.Sprintf("maxSampleCount must be >%d", minMaxRequestsThreshold))
	}

	if hedgingQuantile <= 0.0 || hedgingQuantile >= 1.0 {
		panic("hedingQuantile must be in the range (0.0-1.0) exclusive")
	}

	return hedgingProxy{
		delegate,
		newHedger(initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
		newHedger(initialSlowRequestThreshold, maxSampleCount, hedgingQuantile),
	}
}

func (hp hedgingProxy) Revision(ctx context.Context) (rev datastore.Revision, err error) {
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		rev, err = hp.delegate.Revision(ctx)
		responseReady <- struct{}{}
	}

	hp.revisionHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) SyncRevision(ctx context.Context) (rev datastore.Revision, err error) {
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		rev, err = hp.delegate.SyncRevision(ctx)
		responseReady <- struct{}{}
	}

	hp.syncRevisionHedger(ctx, subreq)

	return
}

func (hp hedgingProxy) ReadNamespace(ctx context.Context, nsName string) (ns *v0.NamespaceDefinition, rev datastore.Revision, err error) {
	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		ns, rev, err = hp.delegate.ReadNamespace(ctx, nsName)
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

func (hp hedgingProxy) QueryTuples(filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	return hedgingTupleQuery{
		hp.delegate.QueryTuples(filter, revision),
		hp.queryTuplesHedger,
	}
}

func (hp hedgingProxy) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return hedgingReverseTupleQuery{
		hp.delegate.ReverseQueryTuplesFromSubjectNamespace(subjectNamespace, revision),
		hp.queryTuplesHedger,
	}
}

func (hp hedgingProxy) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return hedgingReverseTupleQuery{
		hp.delegate.ReverseQueryTuplesFromSubject(subject, revision),
		hp.queryTuplesHedger,
	}
}

func (hp hedgingProxy) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return hedgingReverseTupleQuery{
		hp.delegate.ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation, revision),
		hp.queryTuplesHedger,
	}
}

func (hp hedgingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return hp.delegate.CheckRevision(ctx, revision)
}

func (hp hedgingProxy) ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error) {
	return hp.delegate.ListNamespaces(ctx)
}

type hedgingTupleQuery struct {
	delegate          datastore.TupleQuery
	queryTuplesHedger hedger
}

type tupleExecutor func(ctx context.Context) (datastore.TupleIterator, error)

func executeQuery(ctx context.Context, exec tupleExecutor, queryHedger hedger) (delegateIterator datastore.TupleIterator, err error) {
	doneLock := sync.Mutex{}
	alreadyReturned := false

	subreq := func(ctx context.Context, responseReady chan<- struct{}) {
		tempIterator, tempErr := exec(ctx)
		doneLock.Lock()
		defer doneLock.Unlock()

		if alreadyReturned {
			if tempErr == nil {
				tempIterator.Close()
			}
		} else {
			alreadyReturned = true
			delegateIterator = tempIterator
			err = tempErr
			responseReady <- struct{}{}
		}
	}

	queryHedger(ctx, subreq)

	return
}

func (htq hedgingTupleQuery) Execute(ctx context.Context) (delegateIterator datastore.TupleIterator, err error) {
	return executeQuery(ctx, htq.delegate.Execute, htq.queryTuplesHedger)
}

func (htq hedgingTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	return hedgingCommonTupleQuery{
		htq.delegate.Limit(limit),
		htq.queryTuplesHedger,
	}
}

func (htq hedgingTupleQuery) WithSubjectFilter(filter *v1.SubjectFilter) datastore.TupleQuery {
	htq.delegate = htq.delegate.WithSubjectFilter(filter)
	return htq
}

func (htq hedgingTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	htq.delegate = htq.delegate.WithUsersets(usersets)
	return htq
}

type hedgingReverseTupleQuery struct {
	delegate          datastore.ReverseTupleQuery
	queryTuplesHedger hedger
}

func (hrtq hedgingReverseTupleQuery) Execute(ctx context.Context) (delegateIterator datastore.TupleIterator, err error) {
	return executeQuery(ctx, hrtq.delegate.Execute, hrtq.queryTuplesHedger)
}

func (hrtq hedgingReverseTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	return hedgingCommonTupleQuery{
		hrtq.delegate.Limit(limit),
		hrtq.queryTuplesHedger,
	}
}

func (hrtq hedgingReverseTupleQuery) WithObjectRelation(namespace string, relation string) datastore.ReverseTupleQuery {
	hrtq.delegate = hrtq.delegate.WithObjectRelation(namespace, relation)
	return hrtq
}

type hedgingCommonTupleQuery struct {
	delegate          datastore.CommonTupleQuery
	queryTuplesHedger hedger
}

func (hctq hedgingCommonTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	hctq.delegate = hctq.delegate.Limit(limit)
	return hctq
}

func (hctq hedgingCommonTupleQuery) Execute(ctx context.Context) (delegateIterator datastore.TupleIterator, err error) {
	return executeQuery(ctx, hctq.delegate.Execute, hctq.queryTuplesHedger)
}
