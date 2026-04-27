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
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			rel := tuple.MustParse("resource:foo#viewer@user:tom")
			rel.OptionalIntegrity = &core.RelationshipIntegrity{}
			return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
				tuple.Create(rel),
			})
		})
	})
}

func TestReadWithMissingIntegrity(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	// Write a relationship to the underlying datastore without integrity information.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		rel := tuple.MustParse("resource:foo#viewer@user:tom")
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(rel),
		})
	})
	require.NoError(t, err)

	// Attempt to read, which should return an error.
	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "is missing required integrity data")
}

func TestBasicIntegrityFailureDueToInvalidHashVersion(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
			tuple.Touch(tuple.MustParse("resource:bar#viewer@user:sarah")),
		})
	})
	require.NoError(t, err)

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the proxy.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     []byte("invalidhash"),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	// Read them back and ensure the read fails.
	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "has invalid integrity data")
}

func TestBasicIntegrityFailureDueToInvalidHashSignature(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
			tuple.Touch(tuple.MustParse("resource:bar#viewer@user:sarah")),
		})
	})
	require.NoError(t, err)

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     append([]byte{0x01}, []byte("someinvalidhashaasd")[0:hashLength]...),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	// Read them back and ensure the read fails.
	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "has invalid integrity hash")
}

func TestBasicIntegrityFailureDueToWriteWithExpiredKey(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	// Create a proxy with the to-be-expired key and write some relationships.
	epds, err := NewRelationshipIntegrityProxy(ds, toBeExpiredKeyForTesting, nil)
	require.NoError(t, err)

	// Write some relationships.
	_, err = epds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
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
	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "is signed by an expired key")
}

func TestWatchIntegrityFailureDueToInvalidHashSignature(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	headRev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	watchEvents, errChan := pds.Watch(t.Context(), headRev, datastore.WatchJustRelationships())

	// Insert an invalid integrity hash for one of the relationships to be invalid by bypassing
	// the proxy.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalidTpl := tuple.MustParse("resource:foo#viewer@user:jimmy")
		invalidTpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     append([]byte{0x01}, []byte("someinvalidhashaasd")[0:hashLength]...),
			HashedAt: timestamppb.Now(),
		}

		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(invalidTpl),
		})
	})
	require.NoError(t, err)

	select {
	case _, ok := <-watchEvents:
		require.False(t, ok, "expected immediate channel closure")

	case err := <-errChan:
		require.Error(t, err)
		require.ErrorContains(t, err, "has invalid integrity hash")

	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for watch error")
	}
}

func TestNewRelationshipIntegrityProxyErrors(t *testing.T) {
	validKey := DefaultKeyForTesting

	expTime, err := time.Parse("2006-01-02", "2021-01-01")
	require.NoError(t, err)

	cases := []struct {
		name        string
		current     KeyConfig
		expired     []KeyConfig
		errContains string
	}{
		{
			name:        "empty current key bytes",
			current:     KeyConfig{ID: "k1", Bytes: nil},
			errContains: "current key file cannot be empty",
		},
		{
			name:        "empty current key ID",
			current:     KeyConfig{ID: "", Bytes: validKey.Bytes},
			errContains: "current key ID cannot be empty",
		},
		{
			name:    "expired key empty bytes",
			current: validKey,
			expired: []KeyConfig{
				{ID: "exp", Bytes: nil, ExpiredAt: &expTime},
			},
			errContains: "expired key cannot be empty",
		},
		{
			name:    "expired key empty ID",
			current: validKey,
			expired: []KeyConfig{
				{ID: "", Bytes: validKey.Bytes, ExpiredAt: &expTime},
			},
			errContains: "expired key ID cannot be empty",
		},
		{
			name:    "expired key missing expiration",
			current: validKey,
			expired: []KeyConfig{
				{ID: "exp", Bytes: validKey.Bytes, ExpiredAt: nil},
			},
			errContains: "expired key missing expiration time",
		},
		{
			name:    "duplicate key ID",
			current: validKey,
			expired: []KeyConfig{
				{ID: validKey.ID, Bytes: validKey.Bytes, ExpiredAt: &expTime},
			},
			errContains: "found duplicate key ID",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
			require.NoError(t, err)

			_, err = NewRelationshipIntegrityProxy(ds, tc.current, tc.expired)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.errContains)
		})
	}

	// "Current key has an expiration" panics via MustBugf rather than
	// returning an error, so test it separately.
	t.Run("current key with expiration panics", func(t *testing.T) {
		ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
		require.NoError(t, err)

		require.Panics(t, func() {
			_, _ = NewRelationshipIntegrityProxy(ds, KeyConfig{
				ID:        "k1",
				Bytes:     validKey.Bytes,
				ExpiredAt: &expTime,
			}, nil)
		})
	})
}

func TestRelationshipIntegrityProxyPassThroughs(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	ctx := t.Context()

	metricsID, err := pds.MetricsID()
	require.NoError(t, err)
	require.NotEmpty(t, metricsID)

	uniqueID, err := pds.UniqueID(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, uniqueID)

	features, err := pds.Features(ctx)
	require.NoError(t, err)
	require.NotNil(t, features)

	offline, err := pds.OfflineFeatures()
	require.NoError(t, err)
	require.NotNil(t, offline)

	headRev, err := pds.HeadRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, headRev)

	require.NoError(t, pds.CheckRevision(ctx, headRev))

	optRev, err := pds.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	readyState, err := pds.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	roundTripped, err := pds.RevisionFromString(headRev.String())
	require.NoError(t, err)
	require.True(t, roundTripped.Equal(headRev))

	_, err = pds.Statistics(ctx)
	require.NoError(t, err)

	unwrapper, ok := pds.(datastore.UnwrappableDatastore)
	require.True(t, ok)
	require.Equal(t, ds, unwrapper.Unwrap())

	require.NoError(t, pds.Close())
}

func TestRelationshipIntegrityReaderPassThroughs(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)

	// Each of these simply delegates to the wrapped reader; memdb returns
	// empty/zero results at head on a fresh database.
	_, err = reader.CountRelationships(t.Context(), "nonexistent")
	require.Error(t, err) // memdb errors on unknown counter

	caveats, err := reader.LegacyListAllCaveats(t.Context())
	require.NoError(t, err)
	require.Empty(t, caveats)

	namespaces, err := reader.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Empty(t, namespaces)

	lookupC, err := reader.LegacyLookupCaveatsWithNames(t.Context(), []string{"missing"})
	require.NoError(t, err)
	require.Empty(t, lookupC)

	counters, err := reader.LookupCounters(t.Context())
	require.NoError(t, err)
	require.Empty(t, counters)

	lookupN, err := reader.LegacyLookupNamespacesWithNames(t.Context(), []string{"missing"})
	require.NoError(t, err)
	require.Empty(t, lookupN)

	_, _, err = reader.LegacyReadCaveatByName(t.Context(), "missing")
	require.Error(t, err)

	_, _, err = reader.LegacyReadNamespaceByName(t.Context(), "missing")
	require.Error(t, err)
}

func TestRelationshipIntegrityReverseQueryValidatesHash(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	// Write a valid relationship through the proxy.
	_, err = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
		})
	})
	require.NoError(t, err)

	// Bypass the proxy to insert a relationship with an invalid hash.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		invalid := tuple.MustParse("resource:foo#viewer@user:fred")
		invalid.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "defaultfortest",
			Hash:     append([]byte{0x01}, []byte("someinvalidhashaasd")[0:hashLength]...),
			HashedAt: timestamppb.Now(),
		}
		return tx.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
			tuple.Create(invalid),
		})
	})
	require.NoError(t, err)

	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := pds.SnapshotReader(headRev)
	iter, err := reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "has invalid integrity hash")
}

// stubBulkSource is a trivial BulkWriteRelationshipSource used to verify
// that the integrity proxy decorates the iterator correctly.
type stubBulkSource struct {
	rels []tuple.Relationship
	idx  int
}

func (s *stubBulkSource) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.idx >= len(s.rels) {
		return nil, nil
	}
	rel := s.rels[s.idx]
	s.idx++
	return &rel, nil
}

func TestRelationshipIntegrityBulkLoad(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	src := &stubBulkSource{rels: []tuple.Relationship{
		tuple.MustParse("resource:foo#viewer@user:tom"),
		tuple.MustParse("resource:foo#viewer@user:fred"),
	}}

	_, err = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		loaded, err := tx.BulkLoad(ctx, src)
		require.NoError(t, err)
		require.Equal(t, uint64(2), loaded)
		return nil
	})
	require.NoError(t, err)

	// Integrity metadata should have been added and verifiable on readback.
	headRev, err := pds.HeadRevision(t.Context())
	require.NoError(t, err)

	iter, err := pds.SnapshotReader(headRev).QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{OptionalResourceType: "resource"},
	)
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Len(t, rels, 2)
}

func TestRelationshipIntegrityBulkLoadRejectsPrehashed(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)

	pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
	require.NoError(t, err)

	prehashed := tuple.MustParse("resource:foo#viewer@user:tom")
	prehashed.OptionalIntegrity = &core.RelationshipIntegrity{KeyId: "other"}

	src := &stubBulkSource{rels: []tuple.Relationship{prehashed}}

	// spiceerrors.MustBugf panics; BulkLoad propagates that through the iterator's
	// Next call.
	require.Panics(t, func() {
		_, _ = pds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			_, _ = tx.BulkLoad(ctx, src)
			return nil
		})
	})
}

func BenchmarkQueryRelsWithIntegrity(b *testing.B) {
	for _, withIntegrity := range []bool{true, false} {
		b.Run(fmt.Sprintf("withIntegrity=%t", withIntegrity), func(b *testing.B) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(b, 0, 5*time.Second, 1*time.Hour)
			require.NoError(b, err)

			pds, err := NewRelationshipIntegrityProxy(ds, DefaultKeyForTesting, nil)
			require.NoError(b, err)

			_, err = pds.ReadWriteTx(b.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				for i := range 1000 {
					rel := tuple.MustParse(fmt.Sprintf("resource:foo#viewer@user:user-%d", i))
					if err := tx.WriteRelationships(b.Context(), []tuple.RelationshipUpdate{
						tuple.Create(rel),
					}); err != nil {
						return err
					}
				}

				return nil
			})
			require.NoError(b, err)

			headRev, err := pds.HeadRevision(b.Context())
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
					b.Context(),
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
