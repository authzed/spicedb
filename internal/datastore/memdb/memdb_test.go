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
	test "github.com/authzed/spicedb/pkg/datastore/test"
	ns "github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type memDBTest struct{}

func (mdbt memDBTest) New(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
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
		relationList = append(relationList, ns.Relation(fmt.Sprintf("reader%d", i), nil))
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

				return rwt.WriteNamespaces(ns.Namespace(
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

				return rwt.WriteNamespaces(ns.Namespace("user", relationList...))
			})

			return g.Wait()
		})
		return numPanics > 0
	}, 1*time.Second, 10*time.Millisecond)
	require.ErrorIs(err, recoverErr)
}
