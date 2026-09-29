package checkbaseline

import (
	"context"
	"errors"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
	"testing"
)

type fakeReader struct{ datalayer.RevisionedReader }

func (fakeReader) QueryRelationships(context.Context, datastore.RelationshipsFilter, ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return func(yield func(tuple.Relationship, error) bool) {
		if !yield(tuple.MustParse("document:d#viewer@user:u"), nil) {
			return
		}
		yield(tuple.Relationship{}, errors.New("read failed"))
	}, nil
}
func TestReaderAccounting(t *testing.T) {
	for _, partial := range []bool{true, false} {
		rec := NewRecorder()
		ctx := WithRecorder(t.Context(), rec)
		r := &auditReader{RevisionedReader: fakeReader{}}
		seq, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
		require.NoError(t, err)
		for _, err := range seq {
			if partial {
				break
			}
			if err != nil {
				require.EqualError(t, err, "read failed")
			}
		}
		w := rec.Seal()
		require.Len(t, w.Events, 1)
		require.Equal(t, 1, w.Events[0].Rows)
		require.Positive(t, w.Events[0].Bytes)
		if partial {
			require.Empty(t, w.Events[0].Error)
		} else {
			require.Equal(t, "read failed", w.Events[0].Error)
		}
	}
}
func TestDelayCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := &auditReader{RevisionedReader: fakeReader{}, delay: 100000}
	_, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.ErrorIs(t, err, context.Canceled)
}
