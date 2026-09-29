package query

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

// stubReader is a QueryDatastoreReader that returns empty results, used to
// verify that CountingReader counts and delegates correctly. Its own counter is
// atomic so the concurrency case below does not race on it.
type stubReader struct {
	calls atomic.Int64
}

func (s *stubReader) CheckRelationships(_ context.Context, _ ObjectType, _ string, _ string, _ ObjectAndRelation, _, _ bool) (PathSeq, error) {
	s.calls.Add(1)
	return EmptyPathSeq(), nil
}

func (s *stubReader) QuerySubjects(_ context.Context, _ Object, _ string, _ ObjectType, _, _ bool, _ QueryPage) (PathSeq, error) {
	s.calls.Add(1)
	return EmptyPathSeq(), nil
}

func (s *stubReader) QueryResources(_ context.Context, _ string, _ string, _ ObjectAndRelation, _, _ bool, _ QueryPage) (PathSeq, error) {
	s.calls.Add(1)
	return EmptyPathSeq(), nil
}

func (s *stubReader) LookupCaveatDefinition(_ context.Context, _ string) (datastore.CaveatDefinition, error) {
	s.calls.Add(1)
	return nil, nil
}

func TestCountingReader(t *testing.T) {
	docType := ObjectType{Type: "document", Subrelation: "..."}
	doc1 := NewObject("document", "doc1")
	doc2 := NewObject("document", "doc2")
	alice := NewObject("user", "alice").WithEllipses()

	t.Run("counts every call and delegates", func(t *testing.T) {
		require := require.New(t)
		inner := &stubReader{}
		r := NewCountingReader(inner)
		ctx := t.Context()

		_, err := r.CheckRelationships(ctx, docType, "doc1", "viewer", alice, false, false)
		require.NoError(err)
		_, err = r.QuerySubjects(ctx, doc1, "viewer", docType, false, false, QueryPage{})
		require.NoError(err)
		_, err = r.QueryResources(ctx, "document", "viewer", alice, false, false, QueryPage{})
		require.NoError(err)
		require.Equal(3, r.Queries())
		require.Equal(3, r.DistinctQueries())
		require.Equal(3, int(inner.calls.Load()), "every counted call must reach the inner reader")
	})

	t.Run("distinguishes repeats from distinct calls", func(t *testing.T) {
		require := require.New(t)
		r := NewCountingReader(&stubReader{})
		ctx := t.Context()

		// The same query three times, then a different resource.
		for range 3 {
			_, err := r.CheckRelationships(ctx, docType, "doc1", "viewer", alice, false, false)
			require.NoError(err)
		}
		_, err := r.CheckRelationships(ctx, docType, "doc2", "viewer", alice, false, false)
		require.NoError(err)

		require.Equal(4, r.Queries())
		require.Equal(2, r.DistinctQueries(), "doc1 read three times counts once as distinct")
	})

	t.Run("separates the operations from each other", func(t *testing.T) {
		require := require.New(t)
		r := NewCountingReader(&stubReader{})
		ctx := t.Context()

		// Same resource and relation, but three different operations: each is
		// a distinct datastore query and must not collide in the key space.
		_, err := r.CheckRelationships(ctx, docType, "doc1", "viewer", alice, false, false)
		require.NoError(err)
		_, err = r.QuerySubjects(ctx, doc1, "viewer", docType, false, false, QueryPage{})
		require.NoError(err)
		_, err = r.QueryResources(ctx, "document", "viewer", alice, false, false, QueryPage{})
		require.NoError(err)

		require.Equal(3, r.Queries())
		require.Equal(3, r.DistinctQueries())
	})

	t.Run("does not count caveat definition lookups", func(t *testing.T) {
		require := require.New(t)
		inner := &stubReader{}
		r := NewCountingReader(inner)

		_, err := r.LookupCaveatDefinition(t.Context(), "somecaveat")
		require.NoError(err)

		require.Equal(0, r.Queries(), "caveat definitions are cached, not per-query round-trips")
		require.Equal(int64(1), inner.calls.Load(), "the call must still reach the inner reader")
	})

	t.Run("Reset zeroes both counters", func(t *testing.T) {
		require := require.New(t)
		r := NewCountingReader(&stubReader{})
		ctx := t.Context()

		_, err := r.CheckRelationships(ctx, docType, "doc1", "viewer", alice, false, false)
		require.NoError(err)
		require.Equal(1, r.Queries())

		r.Reset()
		require.Equal(0, r.Queries())
		require.Equal(0, r.DistinctQueries())

		// The key space is cleared too, so a repeat after Reset counts as distinct.
		_, err = r.CheckRelationships(ctx, docType, "doc1", "viewer", alice, false, false)
		require.NoError(err)
		require.Equal(1, r.Queries())
		require.Equal(1, r.DistinctQueries())
	})

	t.Run("is safe for concurrent use", func(t *testing.T) {
		require := require.New(t)
		r := NewCountingReader(&stubReader{})
		ctx := t.Context()

		const goroutines = 8
		const perGoroutine = 50

		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range perGoroutine {
					_, _ = r.QuerySubjects(ctx, doc2, "viewer", docType, false, false, QueryPage{})
				}
			})
		}
		wg.Wait()

		require.Equal(goroutines*perGoroutine, r.Queries())
		require.Equal(1, r.DistinctQueries())
	})
}
