package query

import (
	"context"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// delayReader is a QueryDatastoreReader shim that sleeps for a fixed duration
// before each call to simulate network latency. It delegates all logic to an
// inner reader.
type delayReader struct {
	delay time.Duration
	inner QueryDatastoreReader
}

// NewDelayReader wraps inner with a shim that sleeps for delay before every
// call. Use this in benchmarks to model realistic network round-trip costs.
func NewDelayReader(delay time.Duration, inner QueryDatastoreReader) QueryDatastoreReader {
	return &delayReader{delay: delay, inner: inner}
}

func (r *delayReader) sleep(ctx context.Context) error {
	select {
	case <-time.After(r.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *delayReader) CheckRelationships(
	ctx context.Context,
	resources []Object,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.CheckRelationships(ctx, resources, resourceRelation, subject, withCaveats, withExpiration)
}

func (r *delayReader) QuerySubjects(
	ctx context.Context,
	resource Object,
	resourceRelation string,
	subjectType ObjectType,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.QuerySubjects(ctx, resource, resourceRelation, subjectType, withCaveats, withExpiration, page)
}

func (r *delayReader) QueryResources(
	ctx context.Context,
	resourceType string,
	resourceRelation string,
	subject ObjectAndRelation,
	withCaveats, withExpiration bool,
	page QueryPage,
) (PathSeq, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.QueryResources(ctx, resourceType, resourceRelation, subject, withCaveats, withExpiration, page)
}

func (r *delayReader) SubjectExistsAsRelationship(
	ctx context.Context,
	subject Object,
	nonEllipsisRelation string,
) (bool, error) {
	if err := r.sleep(ctx); err != nil {
		return false, err
	}
	return r.inner.SubjectExistsAsRelationship(ctx, subject, nonEllipsisRelation)
}

func (r *delayReader) LookupCaveatDefinition(
	ctx context.Context,
	name string,
) (datastore.CaveatDefinition, error) {
	if err := r.sleep(ctx); err != nil {
		return nil, err
	}
	return r.inner.LookupCaveatDefinition(ctx, name)
}
