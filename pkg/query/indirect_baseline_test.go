package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
)

type indirectRecordingReader struct {
	datalayer.RevisionedReader
	filter datastore.RelationshipsFilter
	opts   *options.QueryOptions
	rels   []tuple.Relationship
}

func (r *indirectRecordingReader) QueryRelationships(_ context.Context, f datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.filter = f
	r.opts = options.NewQueryOptionsWithOptions(opts...)
	return func(yield func(tuple.Relationship, error) bool) {
		for _, rel := range r.rels {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

type indirectCustomReader struct{ QueryDatastoreReader }

func TestBroadSingleUsersetReadGuards(t *testing.T) {
	for _, tc := range []struct {
		name, allowed                              string
		enabled, paged, broad, caveats, expiration bool
	}{
		{"single", "user | group#member", true, false, true, false, false},
		{"default unchanged", "user | group#member", false, false, false, false, false},
		{"multiple types", "group#member | other#member", true, false, false, false, false},
		{"multiple relations", "group#member | group#admin", true, false, false, false, false},
		{"empty resource ID", "group#member", true, false, false, false, false},
		{"empty resource type", "group#member", true, false, false, false, false},
		{"custom reader", "group#member", true, false, false, false, false},
		{"pagination", "group#member", true, true, false, false, false},
		{"mixed indirect traits", "user | group#member | group#member with condition | group#member with expiration", true, false, true, true, true},
		{"direct traits excluded", "user with condition and expiration | user:* | group#member", true, false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := fmt.Sprintf("use expiration\ncaveat condition(ok bool) { ok }\ndefinition user {}\ndefinition group { relation member: user\nrelation admin: user\n}\ndefinition other { relation member: user\n}\ndefinition document { relation viewer: %s\n}", tc.allowed)
			compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("test"), SchemaString: text}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)
			s, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
			require.NoError(t, err)
			base, err := s.ResolveBaseRelation("document", "viewer", "group", "member", "", false, false)
			require.NoError(t, err)
			reader := &indirectRecordingReader{}
			ctx := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithCheckExecution(CheckExecutionOptions{BroadSingleUsersetReads: tc.enabled}))
			resource := NewObject("document", "doc")
			switch tc.name {
			case "empty resource ID":
				resource.ObjectID = ""
			case "empty resource type":
				resource.ObjectType = ""
			case "custom reader":
				ctx.Reader = &indirectCustomReader{ctx.Reader}
			}
			if tc.paged {
				n := uint64(10)
				ctx.PaginationLimit = &n
			}
			seq, err := NewDatastoreIterator(base).IterSubjectsImpl(ctx, resource, ObjectType{})
			require.NoError(t, err)
			_, err = CollectAll(seq)
			require.NoError(t, err)
			if resource.ObjectID == "" {
				require.Empty(t, reader.filter.OptionalResourceIds)
			}
			selector := reader.filter.OptionalSubjectsSelectors[0]
			require.Equal(t, tc.broad, selector.RelationFilter.OnlyNonEllipsisRelations)
			if tc.broad {
				require.Empty(t, selector.OptionalSubjectType)
				require.Equal(t, queryshape.CheckPermissionSelectIndirectSubjects, reader.opts.QueryShape)
				require.Equal(t, !tc.caveats, reader.opts.SkipCaveats)
				require.Equal(t, !tc.expiration, reader.opts.SkipExpiration)
				// Broader datastore selection must not leak stale nonconforming subjects
				// into this leaf's semantic result.
				reader.rels = []tuple.Relationship{tuple.MustParse("document:doc#viewer@other:stale#member"), tuple.MustParse("document:doc#viewer@group:valid#member")}
				if tc.caveats {
					reader.rels[1].OptionalCaveat = &core.ContextualizedCaveat{CaveatName: "condition"}
				}
				if tc.expiration {
					expires := time.Now().Add(time.Hour)
					reader.rels[1].OptionalExpiration = &expires
				}
				seq, err = NewDatastoreIterator(base).IterSubjectsImpl(ctx, resource, ObjectType{})
				require.NoError(t, err)
				paths, err := CollectAll(seq)
				require.NoError(t, err)
				require.Len(t, paths, 1)
				require.Equal(t, "valid", paths[0].Subject.ObjectID)
				require.Equal(t, tc.caveats, paths[0].Caveat != nil)
				require.Equal(t, reader.rels[1].OptionalExpiration, paths[0].Expiration)
			} else {
				require.Equal(t, "group", selector.OptionalSubjectType)
				require.Equal(t, queryshape.Varying, reader.opts.QueryShape)
			}
		})
	}
}
