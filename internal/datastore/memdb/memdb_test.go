package memdb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	test "github.com/authzed/spicedb/pkg/datastore/test"
	ns "github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var memdbFactory = test.NewTesterFactory(ErrSerialization)

type memDBTest struct{}

func (memDBTest) New(_ testing.TB, revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return NewMemdbDatastore(watchBufferLength, revisionQuantization, gcWindow)
}

func TestMemdbDatastore(t *testing.T) {
	test.All(t, memdbFactory.NewTester(memDBTest{}))
}

func TestConcurrentWritePanic(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	ctx := t.Context()
	recoverErr := errors.New("panic")

	// Make the namespace very large to increase the likelihood of overlapping
	relationList := make([]*corev1.Relation, 0, 1000)
	for i := range 1000 {
		relationList = append(relationList, ns.MustRelation(fmt.Sprintf("reader%d", i), nil))
	}

	numPanics := uint64(0)
	require.Eventually(func() bool {
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			g := errgroup.Group{}
			g.Go(func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						atomic.AddUint64(&numPanics, 1)
						err = recoverErr
					}
				}()

				return rwt.LegacyWriteNamespaces(ctx, ns.Namespace(
					"resource",
					relationList...,
				))
			})

			g.Go(func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						atomic.AddUint64(&numPanics, 1)
						err = recoverErr
					}
				}()

				return rwt.LegacyWriteNamespaces(ctx, ns.Namespace("user", relationList...))
			})

			return g.Wait()
		})
		return numPanics > 0
	}, 3*time.Second, 10*time.Millisecond)
	require.ErrorIs(err, recoverErr)
}

func TestConcurrentWriteRelsError(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	ctx := t.Context()

	// Kick off a number of writes to ensure at least one hits an error.
	g := errgroup.Group{}

	for i := range 50 {
		g.Go(func() error {
			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				updates := []tuple.RelationshipUpdate{}
				for j := range 500 {
					updates = append(updates, tuple.Touch(tuple.MustParse(fmt.Sprintf("document:doc-%d-%d#viewer@user:tom", i, j))))
				}

				return rwt.WriteRelationships(ctx, updates)
			}, options.WithDisableRetries(true))
			return err
		})
	}

	werr := g.Wait()
	require.Error(werr)
	require.ErrorContains(werr, "serialization max retries exceeded")
}

func TestAnythingAfterCloseDoesNotPanic(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	lowestRevision, err := ds.HeadRevision(t.Context())
	require.NoError(err)

	err = ds.Close()
	require.NoError(err)

	_, errChan := ds.Watch(t.Context(), lowestRevision.Revision, datastore.WatchJustRelationships())

	select {
	case err := <-errChan:
		require.ErrorIs(err, ErrMemDBIsClosed)
	case <-time.After(time.Second):
		require.Fail("expected an error but waited too long")
	}

	_, err = ds.Statistics(t.Context())
	require.ErrorIs(err, ErrMemDBIsClosed)

	err = ds.CheckRevision(t.Context(), lowestRevision.Revision)
	require.ErrorIs(err, ErrMemDBIsClosed)

	_, err = ds.OptimizedRevision(t.Context())
	require.ErrorIs(err, ErrMemDBIsClosed)

	reader := ds.SnapshotReader(datastore.NoRevision)
	_, err = reader.CountRelationships(t.Context(), "blah")
	require.ErrorIs(err, ErrMemDBIsClosed)
}
