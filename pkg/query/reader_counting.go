package query

import (
	"context"
	"strings"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
)

// CountingReader is a QueryDatastoreReader shim that records how many datastore
// round-trips a query execution performs. It delegates all logic to an inner
// reader.
//
// Round-trips — not wall-clock — are the metric that matters for workloads
// bounded by datastore connections, where the pool is the scarce resource. Use
// this in benchmarks to report queries-per-operation alongside ns/op, so a
// change that trades one query for many is visible even when an in-memory
// datastore makes it look free. It composes with NewDelayReader: wrap the
// counter outermost to count the same calls the delay shim slows down.
//
// Queries reports every call; DistinctQueries reports the number of distinct
// (filter, arguments) tuples among them. A large gap between the two means the
// execution is re-reading identical rows within a single request.
//
// CountingReader is safe for concurrent use.
type CountingReader struct {
	inner QueryDatastoreReader

	mu       sync.Mutex
	queries  int             // GUARDED_BY(mu)
	counts   ReaderCounts    // GUARDED_BY(mu)
	distinct map[string]bool // GUARDED_BY(mu)
}

// ReaderCounts breaks the query total down by operation, so a test can assert
// on the queries a change is about without being perturbed by the others.
type ReaderCounts struct {
	Checks    int
	Subjects  int
	Resources int
}

// Total returns the sum of all counted operations, equal to Queries().
func (c ReaderCounts) Total() int {
	return c.Checks + c.Subjects + c.Resources
}

var _ QueryDatastoreReader = &CountingReader{}

// NewCountingReader wraps inner with a shim that counts every datastore call.
func NewCountingReader(inner QueryDatastoreReader) *CountingReader {
	return &CountingReader{inner: inner, distinct: make(map[string]bool)}
}

// record notes a single datastore round-trip against the given query key and
// bumps the per-operation counter that bump points at.
func (r *CountingReader) record(bump func(*ReaderCounts), parts ...string) {
	key := strings.Join(parts, "|")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries++
	bump(&r.counts)
	r.distinct[key] = true
}

// Counts returns the per-operation breakdown of the queries made so far.
func (r *CountingReader) Counts() ReaderCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts
}

func (r *CountingReader) Queries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queries
}

// DistinctQueries returns the number of distinct datastore calls made so far.
// Queries() minus this value is the number of calls that re-read rows an
// earlier call in the same execution already read.
func (r *CountingReader) DistinctQueries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.distinct)
}

// Reset zeroes the counters, so a benchmark can exclude warm-up iterations.
func (r *CountingReader) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = 0
	r.counts = ReaderCounts{}
	clear(r.distinct)
}

func (r *CountingReader) CheckRelationships(
	ctx context.Context,
	resourceType ObjectType,
	resourceID string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
) (PathSeq, error) {
	r.record(func(c *ReaderCounts) { c.Checks++ }, "check", resourceType.Type, resourceID, resourceRelation, subject.String())
	return r.inner.CheckRelationships(ctx, resourceType, resourceID, resourceRelation, subject, withCaveats, withExpiration)
}

func (r *CountingReader) QuerySubjects(
	ctx context.Context,
	resource Object,
	resourceRelation string,
	subjectType ObjectType,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	r.record(func(c *ReaderCounts) { c.Subjects++ }, "subjects", resource.ObjectType, resource.ObjectID, resourceRelation, subjectType.String())
	return r.inner.QuerySubjects(ctx, resource, resourceRelation, subjectType, withCaveats, withExpiration, page)
}

func (r *CountingReader) QueryResources(
	ctx context.Context,
	resourceType string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	r.record(func(c *ReaderCounts) { c.Resources++ }, "resources", resourceType, resourceRelation, subject.String())
	return r.inner.QueryResources(ctx, resourceType, resourceRelation, subject, withCaveats, withExpiration, page)
}

// LookupCaveatDefinition is not counted: implementations are expected to cache
// caveat definitions, so it is not a per-query round-trip.
func (r *CountingReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	return r.inner.LookupCaveatDefinition(ctx, name)
}
