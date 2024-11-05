package proxy

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var DefaultKeyForTesting = KeyConfig{
	ID: "defaultfortest",
	Bytes: (func() []byte {
		b, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000")
		if err != nil {
			panic(err)
		}
		return b
	})(),
	ExpiredAt: nil,
}

var toBeExpiredKeyForTesting = KeyConfig{
	ID: "expiredkeyfortest",
	Bytes: (func() []byte {
		b, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201222")
		if err != nil {
			panic(err)
		}
		return b
	})(),
}

var expiredKeyForTesting = KeyConfig{
	ID: "expiredkeyfortest",
	Bytes: (func() []byte {
		b, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201222")
		if err != nil {
			panic(err)
		}
		return b
	})(),
	ExpiredAt: (func() *time.Time {
		t, err := time.Parse("2006-01-02", "2021-01-01")
		if err != nil {
			panic(err)
		}
		return &t
	})(),
}

func TestWriteWithPredefinedIntegrity(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = pds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			rel := tuple.MustParse("resource:foo#viewer@user:tom")
			rel.OptionalIntegrity = &core.RelationshipIntegrity{}
			return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
				tuple.Create(rel),
			})
		})
	})
}

func TestReadWithMissingIntegrity(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	// Write a relationship to the underlying datastore without integrity information.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		rel := tuple.MustParse("resource:foo#viewer@user:tom")
		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(rel),
		})
	})
	require.NoError(t, err)

	// Attempt to read, which should return an error.
	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	headRev, err := pds.HeadRevision(context.Background())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		context.Background(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "is missing required integrity data")
}

func TestBasicIntegrityFailureDueToInvalidHashVersion(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = pds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
			tuple.Touch(tuple.MustParse("resource:bar#viewer@user:sarah")),
		})
	})
	require.NoError(t, err)

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the proxy.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     []byte("invalidhash"),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	// Read them back and ensure the read fails.
	headRev, err := pds.HeadRevision(context.Background())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		context.Background(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "has invalid integrity data")
}

func TestBasicIntegrityFailureDueToInvalidHashSignature(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = pds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
			tuple.Touch(tuple.MustParse("resource:bar#viewer@user:sarah")),
		})
	})
	require.NoError(t, err)

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     append([]byte{0x01}, []byte("someinvalidhashaasd")[0:hashLength]...),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	// Read them back and ensure the read fails.
	headRev, err := pds.HeadRevision(context.Background())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		context.Background(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "has invalid integrity hash")
}

func TestBasicIntegrityFailureDueToWriteWithExpiredKey(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	// Create a proxy with the to-be-expired key and write some relationships.
	epds, err := NewRelationshipIntegrityProxy(ds, toBeExpiredKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = epds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
			tuple.Touch(tuple.MustParse("resource:bar#viewer@user:sarah")),
		})
	})
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, []KeyConfig{
		expiredKeyForTesting,
	})
	require.NoError(t, err)

	// Read them back and ensure the read fails.
	headRev, err := pds.HeadRevision(context.Background())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		context.Background(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "is signed by an expired key")
}

func TestWatchIntegrityFailureDueToInvalidHashSignature(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	headRev, err := ds.HeadRevision(context.Background())
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	watchEvents, errChan := pds.Watch(context.Background(), headRev, datastore.WatchJustRelationships())

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the proxy.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     append([]byte{0x01}, []byte("someinvalidhashaasd")[0:hashLength]...),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	// Ensure a watch error is raised.
	select {
	case <-watchEvents:
		require.Fail(t, "watch event received")

	case err := <-errChan:
		require.Error(t, err)
		require.ErrorContains(t, err, "has invalid integrity hash")

	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for watch error")
	}
}

func BenchmarkQueryRelsWithIntegrity(b *testing.B) {
	for _, withIntegrity := range []bool{true, false} {
		b.Run(fmt.Sprintf("withIntegrity=%t", withIntegrity), func(b *testing.B) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 5*time.Second, 1*time.Hour)
			require.NoError(b, err)

			pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
			require.NoError(b, err)

			_, err = pds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				for i := 0; i < 1000; i++ {
					rel := tuple.MustParse(fmt.Sprintf("resource:foo#viewer@user:user-%d", i))
					if err := tx.WriteRelationships(context.Background(), []tuple.RelationshipUpdate{
						tuple.Create(rel),
					}); err != nil {
						return err
					}
				}

				return nil
			})
			require.NoError(b, err)

			headRev, err := pds.HeadRevision(context.Background())
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var reader datastore.Reader
				if withIntegrity {
					reader = pds.SnapshotReader(headRev)
				} else {
					reader = ds.SnapshotReader(headRev)
				}
				iter, err := reader.QueryRelationships(
					context.Background(),
					datastore.RelationshipsFilter{OptionalResourceType: "resource"},
				)
				require.NoError(b, err)

				_, err = datastore.IteratorToSlice(iter)
				require.NoError(b, err)
			}
			b.StopTimer()
		})
	}
}

func BenchmarkComputeRelationshipHash(b *testing.B) {
	config := &hmacConfig{
		keyID: "defaultfortest",
		pool:  poolForKey(DefaultKeyForTesting.Bytes),
	}

	rel := tuple.MustParse("resource:foo#viewer@user:tom")
	for i := 0; i < b.N; i++ {
		_, err := computeRelationshipHash(rel, config)
		require.NoError(b, err)
	}
}
