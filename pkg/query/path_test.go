package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPath_ResourceOAR(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	path := &Path{
		Resource: NewObject("document", "doc1"),
		Relation: "viewer",
	}

	oar := path.ResourceOAR()
	require.Equal("document", oar.ObjectType)
	require.Equal("doc1", oar.ObjectID)
	require.Equal("viewer", oar.Relation)
}

func TestPath_IsExpired(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("nil_expiration", func(t *testing.T) {
		t.Parallel()
		path := &Path{}
		require.False(path.IsExpired())
	})

	t.Run("future_expiration", func(t *testing.T) {
		t.Parallel()
		future := time.Now().Add(time.Hour)
		path := &Path{Expiration: &future}
		require.False(path.IsExpired())
	})

	t.Run("past_expiration", func(t *testing.T) {
		t.Parallel()
		past := time.Now().Add(-time.Hour)
		path := &Path{Expiration: &past}
		require.True(path.IsExpired())
	})
}

func TestPath_MergeOr(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("same_resource_and_subject", func(t *testing.T) {
		t.Parallel()
		caveat1 := caveats.CaveatExprForTesting("caveat1")
		caveat2 := caveats.CaveatExprForTesting("caveat2")

		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat1,
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat2,
		}

		err := path1.MergeOr(path2)
		require.NoError(err)

		// Should OR the caveats
		expectedCaveat := caveats.Or(caveat1, caveat2)
		require.True(path1.Caveat.EqualVT(expectedCaveat))
	})

	t.Run("different_resources", func(t *testing.T) {
		t.Parallel()
		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
		}

		path2 := &Path{
			Resource: NewObject("document", "doc2"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
		}

		err := path1.MergeOr(path2)
		require.Error(err)
		require.Contains(err.Error(), "cannot merge paths with different resources")
	})

	t.Run("different_subjects", func(t *testing.T) {
		t.Parallel()
		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("user", "bob", ""),
		}

		err := path1.MergeOr(path2)
		require.Error(err)
		require.Contains(err.Error(), "cannot merge paths with different subjects")
	})
}

func TestPath_MergeAnd(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("basic_merge", func(t *testing.T) {
		t.Parallel()
		caveat1 := caveats.CaveatExprForTesting("caveat1")
		caveat2 := caveats.CaveatExprForTesting("caveat2")

		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat1,
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat2,
		}

		err := path1.MergeAnd(path2)
		require.NoError(err)

		// Should AND the caveats
		expectedCaveat := caveats.And(caveat1, caveat2)
		require.True(path1.Caveat.EqualVT(expectedCaveat))
	})
}

func TestPath_MergeAndNot(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("basic_merge", func(t *testing.T) {
		t.Parallel()
		caveat1 := caveats.CaveatExprForTesting("caveat1")
		caveat2 := caveats.CaveatExprForTesting("caveat2")

		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat1,
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   caveat2,
		}

		err := path1.MergeAndNot(path2)
		require.NoError(err)

		// Should subtract the caveats
		expectedCaveat := caveats.Subtract(caveat1, caveat2)
		require.True(path1.Caveat.EqualVT(expectedCaveat))
	})
}

func TestPath_mergeFrom(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("relation_handling", func(t *testing.T) {
		t.Parallel()
		t.Run("same_relation_preserved", func(t *testing.T) {
			t.Parallel()
			path1 := &Path{
				Resource: NewObject("document", "doc1"),
				Relation: "viewer",
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			path2 := &Path{
				Resource: NewObject("document", "doc1"),
				Relation: "viewer",
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			err := path1.MergeOr(path2)
			require.NoError(err)
			require.Equal("viewer", path1.Relation)
		})

		t.Run("different_relation_cleared", func(t *testing.T) {
			t.Parallel()
			path1 := &Path{
				Resource: NewObject("document", "doc1"),
				Relation: "viewer",
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			path2 := &Path{
				Resource: NewObject("document", "doc1"),
				Relation: "editor",
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			err := path1.MergeOr(path2)
			require.NoError(err)
			require.Equal("", path1.Relation)
		})
	})

	t.Run("expiration_handling", func(t *testing.T) {
		t.Parallel()
		t.Run("nil_expiration_both", func(t *testing.T) {
			t.Parallel()
			path1 := &Path{
				Resource: NewObject("document", "doc1"),
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			path2 := &Path{
				Resource: NewObject("document", "doc1"),
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			err := path1.MergeOr(path2)
			require.NoError(err)
			require.Nil(path1.Expiration)
		})

		t.Run("nil_expiration_first", func(t *testing.T) {
			t.Parallel()
			later := time.Now().Add(time.Hour)
			path1 := &Path{
				Resource: NewObject("document", "doc1"),
				Subject:  NewObjectAndRelation("alice", "user", ""),
			}

			path2 := &Path{
				Resource:   NewObject("document", "doc1"),
				Subject:    NewObjectAndRelation("alice", "user", ""),
				Expiration: &later,
			}

			err := path1.MergeOr(path2)
			require.NoError(err)
			require.NotNil(path1.Expiration)
			require.Equal(later, *path1.Expiration)
		})

		t.Run("earlier_expiration_wins", func(t *testing.T) {
			t.Parallel()
			earlier := time.Now().Add(time.Hour)
			later := time.Now().Add(2 * time.Hour)

			path1 := &Path{
				Resource:   NewObject("document", "doc1"),
				Subject:    NewObjectAndRelation("alice", "user", ""),
				Expiration: &later,
			}

			path2 := &Path{
				Resource:   NewObject("document", "doc1"),
				Subject:    NewObjectAndRelation("alice", "user", ""),
				Expiration: &earlier,
			}

			err := path1.MergeOr(path2)
			require.NoError(err)
			require.Equal(earlier, *path1.Expiration)
		})
	})

	t.Run("integrity_handling", func(t *testing.T) {
		t.Parallel()
		integrity1 := &core.RelationshipIntegrity{KeyId: "key1"}
		integrity2 := &core.RelationshipIntegrity{KeyId: "key2"}

		path1 := &Path{
			Resource:  NewObject("document", "doc1"),
			Subject:   NewObjectAndRelation("alice", "user", ""),
			Integrity: []*core.RelationshipIntegrity{integrity1},
		}

		path2 := &Path{
			Resource:  NewObject("document", "doc1"),
			Subject:   NewObjectAndRelation("alice", "user", ""),
			Integrity: []*core.RelationshipIntegrity{integrity2},
		}

		err := path1.MergeOr(path2)
		require.NoError(err)
		require.Len(path1.Integrity, 2)
		require.Equal(integrity1, path1.Integrity[0])
		require.Equal(integrity2, path1.Integrity[1])
	})

	t.Run("metadata_handling", func(t *testing.T) {
		t.Parallel()
		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Metadata: map[string]any{
				"existing": "value1",
				"shared":   "original",
			},
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Metadata: map[string]any{
				"new":    "value2",
				"shared": "overwritten",
			},
		}

		err := path1.MergeOr(path2)
		require.NoError(err)
		require.Len(path1.Metadata, 3)
		require.Equal("value1", path1.Metadata["existing"])
		require.Equal("value2", path1.Metadata["new"])
		require.Equal("overwritten", path1.Metadata["shared"]) // overwritten
	})

	t.Run("metadata_nil_initialization", func(t *testing.T) {
		t.Parallel()
		path1 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Metadata: nil,
		}

		path2 := &Path{
			Resource: NewObject("document", "doc1"),
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Metadata: map[string]any{"key": "value"},
		}

		err := path1.MergeOr(path2)
		require.NoError(err)
		require.NotNil(path1.Metadata)
		require.Equal("value", path1.Metadata["key"])
	})

	t.Run("caveat_operations", func(t *testing.T) {
		t.Parallel()
		caveat1 := caveats.CaveatExprForTesting("caveat1")
		caveat2 := caveats.CaveatExprForTesting("caveat2")

		testCases := []struct {
			name      string
			mergeFunc func(p1, p2 *Path) error
			expected  *core.CaveatExpression
		}{
			{
				name: "or_operation",
				mergeFunc: func(p1, p2 *Path) error {
					return p1.MergeOr(p2)
				},
				expected: caveats.Or(caveat1, caveat2),
			},
			{
				name: "and_operation",
				mergeFunc: func(p1, p2 *Path) error {
					return p1.MergeAnd(p2)
				},
				expected: caveats.And(caveat1, caveat2),
			},
			{
				name: "andnot_operation",
				mergeFunc: func(p1, p2 *Path) error {
					return p1.MergeAndNot(p2)
				},
				expected: caveats.Subtract(caveat1, caveat2),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				path1 := &Path{
					Resource: NewObject("document", "doc1"),
					Subject:  NewObjectAndRelation("alice", "user", ""),
					Caveat:   caveat1,
				}

				path2 := &Path{
					Resource: NewObject("document", "doc1"),
					Subject:  NewObjectAndRelation("alice", "user", ""),
					Caveat:   caveat2,
				}

				err := tc.mergeFunc(path1, path2)
				require.NoError(err)
				require.True(path1.Caveat.EqualVT(tc.expected))
			})
		}
	})
}

func TestFromRelationship(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("basic_conversion", func(t *testing.T) {
		t.Parallel()
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice",
					Relation:   "",
				},
			},
		}

		path := FromRelationship(rel)
		require.Equal("document", path.Resource.ObjectType)
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("viewer", path.Relation)
		require.Equal("user", path.Subject.ObjectType)
		require.Equal("alice", path.Subject.ObjectID)
		require.Nil(path.Caveat)
		require.Nil(path.Expiration)
		require.Empty(path.Integrity)
		require.NotNil(path.Metadata)
	})

	t.Run("with_caveat", func(t *testing.T) {
		t.Parallel()
		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		expiration := time.Now().Add(time.Hour)
		integrity := &core.RelationshipIntegrity{KeyId: "key1"}

		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice",
					Relation:   "",
				},
			},
			OptionalCaveat:     caveat,
			OptionalExpiration: &expiration,
			OptionalIntegrity:  integrity,
		}

		path := FromRelationship(rel)
		require.NotNil(path.Caveat)
		require.Equal(caveat, path.Caveat.GetCaveat())
		require.Equal(expiration, *path.Expiration)
		require.Len(path.Integrity, 1)
		require.Equal(integrity, path.Integrity[0])
	})
}

func TestPath_ToRelationship(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("basic_conversion", func(t *testing.T) {
		t.Parallel()
		path := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Metadata: make(map[string]any),
		}

		rel, err := path.ToRelationship()
		require.NoError(err)
		require.Equal("document", rel.Resource.ObjectType)
		require.Equal("doc1", rel.Resource.ObjectID)
		require.Equal("viewer", rel.Resource.Relation)
		require.Equal("user", rel.Subject.ObjectType)
		require.Equal("alice", rel.Subject.ObjectID)
		require.Nil(rel.OptionalCaveat)
		require.Nil(rel.OptionalExpiration)
		require.Nil(rel.OptionalIntegrity)
	})

	t.Run("with_simple_caveat", func(t *testing.T) {
		t.Parallel()
		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		caveatExpr := caveats.CaveatAsExpr(caveat)
		expiration := time.Now().Add(time.Hour)
		integrity := &core.RelationshipIntegrity{KeyId: "key1"}

		path := &Path{
			Resource:   NewObject("document", "doc1"),
			Relation:   "viewer",
			Subject:    NewObjectAndRelation("alice", "user", ""),
			Caveat:     caveatExpr,
			Expiration: &expiration,
			Integrity:  []*core.RelationshipIntegrity{integrity},
			Metadata:   make(map[string]any),
		}

		rel, err := path.ToRelationship()
		require.NoError(err)
		require.Equal(caveat, rel.OptionalCaveat)
		require.Equal(expiration, *rel.OptionalExpiration)
		require.Equal(integrity, rel.OptionalIntegrity)
	})

	t.Run("empty_relation_error", func(t *testing.T) {
		t.Parallel()
		path := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "", // Empty relation should cause error
			Subject:  NewObjectAndRelation("alice", "user", ""),
		}

		_, err := path.ToRelationship()
		require.Error(err)
		require.Contains(err.Error(), "cannot convert Path with empty Relation")
	})

	t.Run("complex_caveat_error", func(t *testing.T) {
		t.Parallel()
		// Create a complex caveat expression (OR operation)
		caveat1 := caveats.CaveatExprForTesting("caveat1")
		caveat2 := caveats.CaveatExprForTesting("caveat2")
		complexCaveat := caveats.Or(caveat1, caveat2)

		path := &Path{
			Resource: NewObject("document", "doc1"),
			Relation: "viewer",
			Subject:  NewObjectAndRelation("alice", "user", ""),
			Caveat:   complexCaveat,
		}

		_, err := path.ToRelationship()
		require.Error(err)
		require.Contains(err.Error(), "cannot convert Path with complex caveat expression")
	})

	t.Run("multiple_integrity_error", func(t *testing.T) {
		t.Parallel()
		integrity1 := &core.RelationshipIntegrity{KeyId: "key1"}
		integrity2 := &core.RelationshipIntegrity{KeyId: "key2"}

		path := &Path{
			Resource:  NewObject("document", "doc1"),
			Relation:  "viewer",
			Subject:   NewObjectAndRelation("alice", "user", ""),
			Integrity: []*core.RelationshipIntegrity{integrity1, integrity2},
		}

		_, err := path.ToRelationship()
		require.Error(err)
		require.Contains(err.Error(), "cannot convert Path with multiple integrity values")
	})
}

func TestPath_ConversionRoundtrip(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("basic_roundtrip", func(t *testing.T) {
		t.Parallel()
		originalRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice",
					Relation:   "",
				},
			},
		}

		// Convert to Path and back
		path := FromRelationship(originalRel)
		convertedRel, err := path.ToRelationship()
		require.NoError(err)

		// Verify the roundtrip preserves the essential data
		require.Equal(originalRel.Resource, convertedRel.Resource)
		require.Equal(originalRel.Subject, convertedRel.Subject)
		require.Equal(originalRel.OptionalCaveat, convertedRel.OptionalCaveat)
		require.Equal(originalRel.OptionalExpiration, convertedRel.OptionalExpiration)
		require.Equal(originalRel.OptionalIntegrity, convertedRel.OptionalIntegrity)
	})

	t.Run("roundtrip_with_optional_fields", func(t *testing.T) {
		t.Parallel()
		caveat := &core.ContextualizedCaveat{CaveatName: "test_caveat"}
		expiration := time.Now().Add(time.Hour).Truncate(time.Microsecond) // Truncate for comparison
		integrity := &core.RelationshipIntegrity{KeyId: "key1"}

		originalRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice",
					Relation:   "",
				},
			},
			OptionalCaveat:     caveat,
			OptionalExpiration: &expiration,
			OptionalIntegrity:  integrity,
		}

		// Convert to Path and back
		path := FromRelationship(originalRel)
		convertedRel, err := path.ToRelationship()
		require.NoError(err)

		// Verify the roundtrip preserves all data
		require.Equal(originalRel.Resource, convertedRel.Resource)
		require.Equal(originalRel.Subject, convertedRel.Subject)
		require.Equal(originalRel.OptionalCaveat, convertedRel.OptionalCaveat)
		require.Equal(originalRel.OptionalExpiration, convertedRel.OptionalExpiration)
		require.Equal(originalRel.OptionalIntegrity, convertedRel.OptionalIntegrity)
	})
}
