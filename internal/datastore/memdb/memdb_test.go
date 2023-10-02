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

type memDBTest struct{}

func (mdbt memDBTest) New(revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return NewMemdbDatastore(watchBufferLength, revisionQuantization, gcWindow)
}

func TestMemdbDatastore(t *testing.T) {
	test.All(t, memDBTest{})
}

func TestConcurrentWritePanic(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	ctx := context.Background()
	recoverErr := errors.New("panic")

	// Make the namespace very large to increase the likelihood of overlapping
	relationList := make([]*corev1.Relation, 0, 1000)
	for i := 0; i < 1000; i++ {
		relationList = append(relationList, ns.MustRelation(fmt.Sprintf("reader%d", i), nil))
	}

	numPanics := uint64(0)
	require.Eventually(func() bool {
		_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
			g := errgroup.Group{}
			g.Go(func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						atomic.AddUint64(&numPanics, 1)
						err = recoverErr
					}
				}()

				return rwt.WriteNamespaces(ctx, ns.Namespace(
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

				return rwt.WriteNamespaces(ctx, ns.Namespace("user", relationList...))
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

	ctx := context.Background()

	// Kick off a number of writes to ensure at least one hits an error.
	g := errgroup.Group{}

	for i := 0; i < 50; i++ {
		i := i
		g.Go(func() error {
			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				updates := []*corev1.RelationTupleUpdate{}
				for j := 0; j < 500; j++ {
					updates = append(updates, &corev1.RelationTupleUpdate{
						Operation: corev1.RelationTupleUpdate_TOUCH,
						Tuple:     tuple.MustParse(fmt.Sprintf("document:doc-%d-%d#viewer@user:tom", i, j)),
					})
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
