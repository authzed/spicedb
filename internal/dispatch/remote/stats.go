package remote

import (
	"context"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v4"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/tuple"
)

// raceStats holds what races learn about the secondaries. It outlives single requests and is
// shared by every strategy, so traffic routed with any strategy trains the same statistics.
// The race engine records into it; policies read from it.
type raceStats struct {
	// secondaryDigests holds, per request key, the latency of the secondaries' first results.
	secondaryDigests map[string]*digestAndLock

	// unsupported tracks the relations a secondary reported as unsupported.
	unsupported *supportedResourceSubjectTracker
}

func newRaceStats(startingPrimaryHedgingDelay time.Duration) (*raceStats, error) {
	digests := make(map[string]*digestAndLock, len(supportsSecondaries))
	for _, requestKey := range supportsSecondaries {
		digest, err := tdigest.New(tdigest.Compression(defaultTDigestCompression))
		if err != nil {
			return nil, err
		}

		digests[requestKey] = &digestAndLock{
			digest:                      digest,
			startingPrimaryHedgingDelay: startingPrimaryHedgingDelay,
			lock:                        sync.RWMutex{},
		}
	}

	return &raceStats{
		secondaryDigests: digests,
		unsupported:      newSupportedResourceSubjectTracker(),
	}, nil
}

// recordSecondaryFirstResult records the time a secondary took to produce its first result,
// and marks the request's relations as supported. The update runs in its own goroutine, off
// the dispatch's critical path: the digest update is comparatively slow, and both only inform
// future dispatches. Like the digest's TryLock, recording is best-effort.
func (s *raceStats) recordSecondaryFirstResult(
	ctx context.Context,
	reqKey string,
	resource tuple.RelationReference,
	subject tuple.RelationReference,
	latency time.Duration,
) {
	go s.recordSecondarySuccess(ctx, reqKey, resource, subject, latency)
}

func (s *raceStats) recordSecondarySuccess(
	ctx context.Context,
	reqKey string,
	resource tuple.RelationReference,
	subject tuple.RelationReference,
	latency time.Duration,
) {
	s.unsupported.updateForSuccess(resource, subject)
	if dal, ok := s.secondaryDigests[reqKey]; ok {
		dal.addResultTime(ctx, latency)
	}
}

// recordSecondaryError records an error returned by a secondary.
func (s *raceStats) recordSecondaryError(err error) {
	s.unsupported.updateForError(err)
}

// hedgingDelay returns how long a hedged primary should wait: the configured quantile of the
// secondaries' first-result latency, capped at maxDelay. It returns zero if the resource or
// subject is known to be unsupported by the secondaries, and the starting delay while the
// digest has too few samples.
func (s *raceStats) hedgingDelay(
	reqKey string,
	resource tuple.RelationReference,
	subject tuple.RelationReference,
	maxDelay time.Duration,
) time.Duration {
	if s.unsupported.isUnsupported(resource, subject) {
		return 0
	}

	dal, ok := s.secondaryDigests[reqKey]
	if !ok {
		return 0
	}
	return dal.getWaitTime(maxDelay)
}

// digestAndLock is a struct that holds a TDigest and a lock to protect it.
type digestAndLock struct {
	digest                      *tdigest.TDigest // GUARDED_BY(lock)
	startingPrimaryHedgingDelay time.Duration
	lock                        sync.RWMutex
}

// getWaitTime returns the configured percentile of the digest, or a default value if the digest is empty.
func (dal *digestAndLock) getWaitTime(maximumHedgingDelay time.Duration) time.Duration {
	dal.lock.RLock()
	milliseconds := dal.digest.Quantile(defaultHedgerQuantile)
	count := dal.digest.Count()
	dal.lock.RUnlock()

	if milliseconds <= 0 || count < minimumDigestCount {
		return dal.startingPrimaryHedgingDelay
	}

	waitTime := min(time.Duration(milliseconds)*time.Millisecond, maximumHedgingDelay)
	return waitTime
}

func (dal *digestAndLock) addResultTime(ctx context.Context, duration time.Duration) {
	if dal.lock.TryLock() {
		err := dal.digest.Add(float64(duration.Milliseconds()))
		dal.lock.Unlock()
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error when trying to add result time to digest")
		}
	}
}

// supportedResourceSubjectTracker is a struct that tracks the resources and subjects that are
// unsupported by the secondary dispatcher(s). If a resource or subject is unsupported, the primary
// dispatcher will not wait for the secondary dispatchers to return results, as it is extremely
// likely that hedging will not be beneficial.
type supportedResourceSubjectTracker struct {
	unsupportedResources sync.Map
	unsupportedSubjects  sync.Map
}

func newSupportedResourceSubjectTracker() *supportedResourceSubjectTracker {
	return &supportedResourceSubjectTracker{
		unsupportedResources: sync.Map{},
		unsupportedSubjects:  sync.Map{},
	}
}

// isUnsupported returns whether the resource or subject is unsupported by the secondary dispatcher(s).
func (srst *supportedResourceSubjectTracker) isUnsupported(
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
) bool {
	isUnsupportedResource, found := srst.unsupportedResources.Load(resourceTypeAndRelation)
	if found && isUnsupportedResource.(bool) {
		return true
	}

	isUnsupportedSubject, found := srst.unsupportedSubjects.Load(subjectTypeAndRelation)
	return found && isUnsupportedSubject.(bool)
}

// updateForSuccess updates the tracker for a successful dispatch, removing the resource and subject
// from the unsupported set.
func (srst *supportedResourceSubjectTracker) updateForSuccess(
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
) {
	srst.unsupportedResources.CompareAndSwap(resourceTypeAndRelation, true, false)
	srst.unsupportedSubjects.CompareAndSwap(subjectTypeAndRelation, true, false)
}

// updateForError updates the tracker for an error, adding the resource or subject to the unsupported set
// if the error indicates that the resource or subject is unsupported.
func (srst *supportedResourceSubjectTracker) updateForError(err error) {
	// Check for a FAILED_PRECONDITION with error details that indicate an unsupported
	// resource or subject.
	st, ok := status.FromError(err)
	if !ok {
		return
	}

	if st.Code() != codes.FailedPrecondition {
		return
	}

	for _, detail := range st.Details() {
		errDetail, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}

		// If the error is an unsupported resource or subject, add it to the tracker.
		if errDetail.Reason == "UNSUPPORTED_RESOURCE_RELATION" {
			definitionName := errDetail.Metadata["definition_name"]
			relationName := errDetail.Metadata["relation_name"]
			rr := tuple.RR(definitionName, relationName)
			existing, loaded := srst.unsupportedResources.LoadOrStore(rr, true)
			if !loaded || !existing.(bool) {
				srst.unsupportedResources.Store(rr, true)
			}
		}

		if errDetail.Reason == "UNSUPPORTED_SUBJECT_RELATION" {
			definitionName := errDetail.Metadata["definition_name"]
			relationName := errDetail.Metadata["relation_name"]
			rr := tuple.RR(definitionName, relationName)
			existing, loaded := srst.unsupportedSubjects.LoadOrStore(rr, true)
			if !loaded || !existing.(bool) {
				srst.unsupportedSubjects.Store(rr, true)
			}
		}
	}
}
