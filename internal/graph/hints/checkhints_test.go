package hints

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)

func TestHintForEntrypoint(t *testing.T) {
	tcs := []struct {
		name            string
		schema          string
		subjectType     string
		subjectRelation string
		expectedHints   []*v1.CheckHint
	}{
		{
			"computed userset entrypoint",
			`
			definition org {
				relation member: user
				permission is_member = member
			}

			definition resource {
				relation org: org
				permission view = org->is_member
			}`,
			"org",
			"member",
			[]*v1.CheckHint{
				CheckHintForComputedUserset("org", "someid", "member", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			},
		},
		{
			"arrow entrypoint",
			`
			definition org {
				relation member: user
				permission is_member = member
			}

			definition resource {
				relation org: org
				permission view = org->is_member
			}`,
			"org",
			"is_member",
			[]*v1.CheckHint{
				CheckHintForArrow("resource", "someid", "org", "is_member", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			rg := buildReachabilityGraph(t, tc.schema)
			subject := tuple.ParseSubjectONR("user:tom")

			entrypoints, err := rg.OptimizedEntrypointsForSubjectToResource(context.Background(), &core.RelationReference{
				Namespace: tc.subjectType,
				Relation:  tc.subjectRelation,
			}, &core.RelationReference{
				Namespace: "resource",
				Relation:  "view",
			})
			require.NoError(t, err)

			hints := make([]*v1.CheckHint, 0, len(entrypoints))
			for _, ep := range entrypoints {
				if ep.EntrypointKind() == core.ReachabilityEntrypoint_RELATION_ENTRYPOINT {
					continue
				}

				hint, err := HintForEntrypoint(ep, "someid", subject, &v1.ResourceCheckResult{})
				require.NoError(t, err)

				hints = append(hints, hint)
			}

			require.Equal(t, tc.expectedHints, hints)
		})
	}
}

func buildReachabilityGraph(t *testing.T, schema string) *typesystem.ReachabilityGraph {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := datastoremw.ContextWithDatastore(context.Background(), ds)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	// Write the schema.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		for _, nsDef := range compiled.ObjectDefinitions {
			if err := tx.WriteNamespaces(ctx, nsDef); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(err)

	lastRevision, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	reader := ds.SnapshotReader(lastRevision)

	_, vts, err := typesystem.ReadNamespaceAndTypes(ctx, "resource", reader)
	require.NoError(err)

	rg := typesystem.ReachabilityGraphFor(vts)
	return rg
}

func TestCheckHintForComputedUserset(t *testing.T) {
	resourceType := "resourceType"
	resourceID := "resourceID"
	relation := "relation"
	subject := &core.ObjectAndRelation{
		Namespace: "subjectNamespace",
		ObjectId:  "subjectObjectId",
		Relation:  "subjectRelation",
	}
	result := &v1.ResourceCheckResult{
		Membership: v1.ResourceCheckResult_MEMBER,
	}

	checkHint := CheckHintForComputedUserset(resourceType, resourceID, relation, subject, result)

	require.Equal(t, resourceType, checkHint.Resource.Namespace)
	require.Equal(t, resourceID, checkHint.Resource.ObjectId)
	require.Equal(t, relation, checkHint.Resource.Relation)
	require.Equal(t, subject, checkHint.Subject)
	require.Equal(t, result, checkHint.Result)
	require.Empty(t, checkHint.TtuComputedUsersetRelation)

	resourceID, ok := AsCheckHintForComputedUserset(checkHint, resourceType, relation, subject)
	require.True(t, ok)
	require.Equal(t, "resourceID", resourceID)
}

func TestCheckHintForArrow(t *testing.T) {
	resourceType := "resourceType"
	resourceID := "resourceID"
	tuplesetRelation := "tuplesetRelation"
	computedUsersetRelation := "computedUsersetRelation"
	subject := &core.ObjectAndRelation{
		Namespace: "subjectNamespace",
		ObjectId:  "subjectObjectId",
		Relation:  "subjectRelation",
	}
	result := &v1.ResourceCheckResult{
		Membership: v1.ResourceCheckResult_MEMBER,
	}

	checkHint := CheckHintForArrow(resourceType, resourceID, tuplesetRelation, computedUsersetRelation, subject, result)

	require.Equal(t, resourceType, checkHint.Resource.Namespace)
	require.Equal(t, resourceID, checkHint.Resource.ObjectId)
	require.Equal(t, tuplesetRelation, checkHint.Resource.Relation)
	require.Equal(t, subject, checkHint.Subject)
	require.Equal(t, result, checkHint.Result)
	require.Equal(t, computedUsersetRelation, checkHint.TtuComputedUsersetRelation)

	resourceID, ok := AsCheckHintForArrow(checkHint, resourceType, tuplesetRelation, computedUsersetRelation, subject)
	require.True(t, ok)
	require.Equal(t, "resourceID", resourceID)
}

func TestAsCheckHintForComputedUserset(t *testing.T) {
	tcs := []struct {
		name           string
		checkHint      *v1.CheckHint
		handler        func(*v1.CheckHint) (string, bool)
		expectedResult string
	}{
		{
			"matching resource and subject",
			CheckHintForComputedUserset("resourceType", "resourceID", "relation", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom"))
			},
			"resourceID",
		},
		{
			"mismatch subject ID",
			CheckHintForComputedUserset("resourceType", "resourceID", "relation", tuple.ParseSubjectONR("user:anothersubject"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch subject type",
			CheckHintForComputedUserset("resourceType", "resourceID", "relation", tuple.ParseSubjectONR("githubuser:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch subject relation",
			CheckHintForComputedUserset("resourceType", "resourceID", "relation", tuple.ParseSubjectONR("user:tom#foo"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch resource type",
			CheckHintForComputedUserset("anotherType", "resourceID", "relation", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch resource relation",
			CheckHintForComputedUserset("resourceType", "resourceID", "anotherRelation", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom#..."))
			},
			"",
		},
		{
			"mismatch kind",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "clu", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForComputedUserset(ch, "resourceType", "relation", tuple.ParseSubjectONR("user:tom#..."))
			},
			"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			resourceID, ok := tc.handler(tc.checkHint)
			if tc.expectedResult == "" {
				require.False(t, ok)
				return
			}

			require.Equal(t, tc.expectedResult, resourceID)
			require.True(t, ok)
		})
	}
}

func TestAsCheckHintForArrow(t *testing.T) {
	tcs := []struct {
		name           string
		checkHint      *v1.CheckHint
		handler        func(*v1.CheckHint) (string, bool)
		expectedResult string
	}{
		{
			"matching resource and subject",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "cur", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"resourceID",
		},
		{
			"mismatch TTU",
			CheckHintForArrow("resourceType", "resourceID", "anotherttu", "cur", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch computeduserset",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "anothercur", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch subject ID",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "cur", tuple.ParseSubjectONR("user:anothersubject"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch subject type",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "cur", tuple.ParseSubjectONR("githubuser:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch subject relation",
			CheckHintForArrow("resourceType", "resourceID", "ttu", "cur", tuple.ParseSubjectONR("user:tom#something"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch resource type",
			CheckHintForArrow("anotherType", "resourceID", "ttu", "cur", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch resource relation",
			CheckHintForArrow("resourceType", "resourceID", "anotherttu", "cur", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
		{
			"mismatch kind",
			CheckHintForComputedUserset("resourceType", "resourceID", "relation", tuple.ParseSubjectONR("user:tom"), &v1.ResourceCheckResult{}),
			func(ch *v1.CheckHint) (string, bool) {
				return AsCheckHintForArrow(ch, "resourceType", "ttu", "cur", tuple.ParseSubjectONR("user:tom"))
			},
			"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			resourceID, ok := tc.handler(tc.checkHint)
			if tc.expectedResult == "" {
				require.False(t, ok)
				return
			}

			require.Equal(t, tc.expectedResult, resourceID)
			require.True(t, ok)
		})
	}
}
