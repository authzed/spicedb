// Package chunkertest supplies a fake dispatch.Chunker, a recording dispatcher and fixtures
// for tests of the dispatch chunking sites.
package chunkertest

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Schema has a direct userset, a computed userset, an arrow and an intersection arrow.
// The direct userset and the arrows dispatch group#member.
// The computed userset of view dispatches document#viewer.
const Schema = `
definition user {}

definition group {
	relation member: user
}

definition document {
	relation viewer: group#member
	relation parent: group
	permission view = viewer + parent->member
	permission parent_member = parent->member
	permission all_parent_member = parent.all(member)
}`

// Relationships returns the relationships for Schema.
// document:doc1 has the viewers and the parents a1, a2 and b1.
// ByFirstByte puts a1 and a2 in one chunk and b1 in a different chunk.
func Relationships() ([]tuple.Relationship, error) {
	strs := []string{
		"document:doc1#viewer@group:a1#member",
		"document:doc1#viewer@group:a2#member",
		"document:doc1#viewer@group:b1#member",
		"document:doc1#parent@group:a1",
		"document:doc1#parent@group:a2",
		"document:doc1#parent@group:b1",
	}
	rels := make([]tuple.Relationship, 0, len(strs))
	for _, s := range strs {
		rel, err := tuple.Parse(s)
		if err != nil {
			return nil, err
		}
		rels = append(rels, rel)
	}
	return rels, nil
}

// Call is one call to ByFirstByte.Chunk.
type Call struct {
	// Namespace is the namespace argument of the call.
	Namespace string

	// IDs is the ids argument of the call.
	IDs []string

	// MaxChunkSize is the maxChunkSize argument of the call.
	MaxChunkSize uint16
}

// ByFirstByte is a dispatch.Chunker that puts the IDs with the same first byte in one owner group.
// The groups are in order of first appearance, and each group keeps the input order.
// It splits each group into chunks of at most the maximum chunk size.
// Each chunk gets the hint that HintFor returns for its IDs.
type ByFirstByte struct {
	mu    sync.Mutex
	calls []Call // GUARDED_BY(mu)
}

// HintFor returns the hint that ByFirstByte gives to the chunk that contains id.
func HintFor(namespace, id string) dispatch.RoutingHint {
	return dispatch.RoutingHint{Key: dispatch.RoutingKey(namespace + ":" + id[:1]), Owner: id[:1]}
}

// Chunk implements dispatch.Chunker.
func (c *ByFirstByte) Chunk(_ context.Context, namespace string, ids []string, maxChunkSize uint16) []dispatch.Chunk {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Namespace: namespace, IDs: ids, MaxChunkSize: maxChunkSize})
	c.mu.Unlock()

	// Group the IDs by first byte. order keeps the groups in order of first appearance.
	order := make([]string, 0, 2)
	groups := make(map[string][]string, 2)
	for _, id := range ids {
		owner := id[:1]
		if _, ok := groups[owner]; !ok {
			order = append(order, owner)
		}

		groups[owner] = append(groups[owner], id)
	}

	// Split each group by size. All chunks of a group get the hint of the group.
	chunks := make([]dispatch.Chunk, 0, len(order))
	for _, owner := range order {
		hint := HintFor(namespace, groups[owner][0])
		slicez.ForEachChunk(groups[owner], maxChunkSize, func(part []string) {
			chunks = append(chunks, dispatch.Chunk{ResourceIDs: part, Hint: hint})
		})
	}
	return chunks
}

// Calls returns a copy of the calls to Chunk.
func (c *ByFirstByte) Calls() []Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

// Dispatch is one dispatch that Recorder received.
type Dispatch struct {
	// Relation is "<namespace>#<relation>" of the resources.
	Relation string

	// IDs are the resource IDs in sorted order.
	IDs []string

	// Hint is the routing hint of the dispatch context. It is zero when HasHint is false.
	Hint dispatch.RoutingHint

	// HasHint reports whether the dispatch context has a routing hint.
	HasHint bool
}

// errUnsupported is the error of the dispatch methods that Recorder does not support.
var errUnsupported = errors.New("chunkertest: Recorder supports only DispatchCheck and DispatchLookupSubjects")

// Recorder is a dispatch.Dispatcher that records each Check and LookupSubjects dispatch with its routing hint.
// Only Check and LookupSubjects use a dispatch.Chunker, so only they can have a routing hint.
// Expand, LookupResources2, LookupResources3 and QueryPlan dispatches return an error.
// A Check dispatch finds no members. A LookupSubjects dispatch finds no subjects.
// A dispatch that contains FailOn returns an error.
type Recorder struct {
	// FailOn is a resource ID. A dispatch that contains it returns an error. Empty disables the error.
	FailOn string

	mu         sync.Mutex
	dispatches []Dispatch // GUARDED_BY(mu)
}

var _ dispatch.Dispatcher = (*Recorder)(nil)

// record records one dispatch and returns an error if ids contains FailOn.
func (r *Recorder) record(ctx context.Context, rr *core.RelationReference, ids []string) error {
	hint, hasHint := dispatch.RoutingHintFrom(ctx)
	sorted := slices.Sorted(slices.Values(ids))

	r.mu.Lock()
	r.dispatches = append(r.dispatches, Dispatch{
		Relation: tuple.StringCoreRR(rr),
		IDs:      sorted,
		Hint:     hint,
		HasHint:  hasHint,
	})
	r.mu.Unlock()

	if r.FailOn != "" && slices.Contains(ids, r.FailOn) {
		return errors.New("chunkertest: injected failure")
	}
	return nil
}

// DispatchCheck records the dispatch.
func (r *Recorder) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	if err := r.record(ctx, req.ResourceRelation, req.ResourceIds); err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}

	return &v1.DispatchCheckResponse{
		Metadata:            &v1.ResponseMeta{DispatchCount: 1},
		ResultsByResourceId: map[string]*v1.ResourceCheckResult{},
	}, nil
}

// DispatchLookupSubjects records the dispatch with the hint of the stream context.
func (r *Recorder) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return r.record(stream.Context(), req.ResourceRelation, req.ResourceIds)
}

// DispatchExpand returns an error. An expand dispatch routes by the request hash and has no routing hint.
func (r *Recorder) DispatchExpand(context.Context, *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{}}, errUnsupported
}

// DispatchLookupResources2 returns an error.
func (r *Recorder) DispatchLookupResources2(*v1.DispatchLookupResources2Request, dispatch.LookupResources2Stream) error {
	return errUnsupported
}

// DispatchLookupResources3 returns an error.
func (r *Recorder) DispatchLookupResources3(*v1.DispatchLookupResources3Request, dispatch.LookupResources3Stream) error {
	return errUnsupported
}

// DispatchQueryPlan returns an error.
func (r *Recorder) DispatchQueryPlan(*v1.DispatchQueryPlanRequest, dispatch.PlanStream) error {
	return errUnsupported
}

// LookupPlanCheck reports a cache miss. Recorder has no cache.
func (r *Recorder) LookupPlanCheck(context.Context, dispatch.PlanCheckLookup) (*v1.ResultPath, bool, error) {
	return nil, false, nil
}

// Close does nothing.
func (r *Recorder) Close() error {
	return nil
}

// ReadyState reports ready.
func (r *Recorder) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{IsReady: true}
}

// Dispatches returns a copy of the recorded dispatches.
func (r *Recorder) Dispatches() []Dispatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.dispatches)
}
