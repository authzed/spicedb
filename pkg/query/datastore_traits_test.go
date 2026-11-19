package query

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Honor projection options just as the datastore does: a trait skipped here
// cannot be recovered by the iterator after the relationship is read.
type traitProjectionReader struct {
	datalayer.RevisionedReader
	relationship tuple.Relationship
}

func (r *traitProjectionReader) QueryRelationships(_ context.Context, _ datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	projection := options.NewQueryOptionsWithOptions(opts...)
	rel := r.relationship
	if projection.SkipCaveats {
		rel.OptionalCaveat = nil
	}
	if projection.SkipExpiration {
		rel.OptionalExpiration = nil
	}
	return func(yield func(tuple.Relationship, error) bool) { yield(rel, nil) }, nil
}

func TestDatastorePreservesMixedTraits(t *testing.T) {
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("test"), SchemaString: `use expiration
caveat condition(ok bool) { ok }
definition user {}
definition document {
 relation viewer: user | user with condition | user with expiration | user with condition and expiration
}`}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	s, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
	require.NoError(t, err)
	base, err := s.ResolveBaseRelation("document", "viewer", "user", tuple.Ellipsis, "", false, false)
	require.NoError(t, err)
	for _, tc := range []struct {
		name               string
		caveat, expiration bool
	}{
		{"plain", false, false}, {"caveated", true, false}, {"expiring", false, true}, {"both", true, true},
	} {
		for _, operation := range []string{"check", "subjects"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				rel := tuple.MustParse("document:doc#viewer@user:alice")
				if tc.caveat {
					rel.OptionalCaveat = &core.ContextualizedCaveat{CaveatName: "condition"}
				}
				if tc.expiration {
					expires := time.Now().Add(-time.Hour)
					rel.OptionalExpiration = &expires
				}
				ctx := NewLocalContext(t.Context(), WithRevisionedReader(&traitProjectionReader{relationship: rel}))
				it := NewDatastoreIterator(base)
				var path *Path
				if operation == "check" {
					path, err = it.CheckImpl(ctx, NewObject("document", "doc"), NewObject("user", "alice").WithEllipses())
					require.NoError(t, err)
				} else {
					seq, err := it.IterSubjectsImpl(ctx, NewObject("document", "doc"), ObjectType{})
					require.NoError(t, err)
					paths, err := CollectAll(seq)
					require.NoError(t, err)
					require.Len(t, paths, 1)
					path = paths[0]
				}
				require.NotNil(t, path)
				require.Equal(t, tc.caveat, path.Caveat != nil)
				if tc.caveat {
					require.Equal(t, "condition", path.Caveat.GetCaveat().CaveatName)
				}
				require.Equal(t, rel.OptionalExpiration, path.Expiration)
				require.Equal(t, tc.expiration, path.IsExpired())
			})
		}
	}
}
