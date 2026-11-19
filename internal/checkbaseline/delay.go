package checkbaseline

import (
	"context"
	"time"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// delayLayer is independent of auditing so timing never pays for work counters.
type delayLayer struct {
	datalayer.DataLayer
	delay time.Duration
}

func (l *delayLayer) SnapshotReader(rev datastore.Revision, h datalayer.SchemaHash) datalayer.RevisionedReader {
	return &delayReader{RevisionedReader: l.DataLayer.SnapshotReader(rev, h), delay: l.delay}
}

type delayReader struct {
	datalayer.RevisionedReader
	delay time.Duration
}

func (r *delayReader) QueryRelationships(ctx context.Context, f datastore.RelationshipsFilter, o ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	if err := waitDelay(ctx, r.delay); err != nil {
		return nil, err
	}
	return r.RevisionedReader.QueryRelationships(ctx, f, o...)
}

func (r *delayReader) ReverseQueryRelationships(ctx context.Context, f datastore.SubjectsFilter, o ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	if err := waitDelay(ctx, r.delay); err != nil {
		return nil, err
	}
	return r.RevisionedReader.ReverseQueryRelationships(ctx, f, o...)
}
