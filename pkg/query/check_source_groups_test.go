package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestNativeSourceGroupsPreserveGeneratedMembership(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		text := `definition user {}
definition group {
 relation member: user
}
definition team {
 relation member: user
}
definition document {
 relation viewer: user | group#member | team#member
 relation parent: group | team
 permission view = viewer
 permission via = parent->member
}`
		relationships := []tuple.Relationship{}
		expected := map[string]map[string]bool{"view": {}, "via": {}}

		// This oracle uses generated edges directly, without executing a reference
		// iterator. Each document has distinct target groups, preventing accidental
		// sharing from making a wrong resource or subject result appear correct.
		for resource := range 3 {
			for subject := range 2 {
				id := fmt.Sprintf("%d-%d", resource, subject)
				direct := rapid.Bool().Draw(rt, "direct-"+id)
				expected["view"][id] = direct

				if direct {
					relationships = append(relationships, tuple.MustParse(fmt.Sprintf("document:d%d#viewer@user:u%d", resource, subject)))
				}

				for _, kind := range []string{"group", "team"} {
					member := rapid.Bool().Draw(rt, kind+"-member-"+id)
					if member {
						relationships = append(
							relationships,
							tuple.MustParse(fmt.Sprintf("%s:g%d#member@user:u%d", kind, resource, subject)),
						)
					}

					expected["view"][id] = expected["view"][id] || member
					expected["via"][id] = expected["via"][id] || member
				}
			}

			for _, kind := range []string{"group", "team"} {
				relationships = append(relationships,
					tuple.MustParse(fmt.Sprintf("document:d%d#viewer@%s:g%d#member", resource, kind, resource)),
					tuple.MustParse(fmt.Sprintf("document:d%d#parent@%s:g%d", resource, kind, resource)))
			}
		}

		converted, reader := batchTestReader(rt, text, relationships)
		resources := []Object{
			NewObject("document", "d0"),
			NewObject("document", "d1"),
			NewObject("document", "d2"),
			NewObject("document", "d0"),
		}
		subjects := []ObjectAndRelation{
			NewObject("user", "u0").WithEllipses(),
			NewObject("user", "u1").WithEllipses(),
			NewObject("user", "u0").WithEllipses(),
		}

		for _, permission := range []string{"view", "via"} {
			outline, err := BuildOutlineFromSchema(converted, "document", permission, WithSchemaExecutionOrder(), WithDeferredCaveats())
			require.NoError(rt, err)
			it, err := outline.Compile()
			require.NoError(rt, err)
			_, err = Walk(it, func(node Iterator) (Iterator, error) {
				if union, ok := node.(*UnionIterator); ok {
					union.checkConcurrency = 3
				}

				return node, nil
			})
			require.NoError(rt, err)

			for _, width := range []uint16{1, 3} {
				// Use the underlying revisioned reader: the test recording wrapper's
				// mutable slices intentionally do not support concurrent callbacks.
				ctx := NewLocalContext(rt.Context(), WithRevisionedReader(
					reader.RevisionedReader,
				), WithBatchedArrows(
					true,
				), WithCheckConcurrency(
					width,
				), WithCheckExecution(CheckExecutionOptions{
					BatchChecks: true, StrictSubjectMatching: true, UnfilteredSingleTypeReads: true, BroadSingleUsersetReads: true,
				}))

				for _, subject := range subjects {
					actual, err := ctx.CheckManyResources(it, resources, subject)
					require.NoError(rt, err)
					partial, err := ctx.CheckManyResources(it, resources, subject, AllowSingleResult)
					require.NoError(rt, err)
					assertAnyBatchEquivalent(rt, actual, partial)

					for index, resource := range resources {
						id := resource.ObjectID[1:] + "-" + subject.ObjectID[1:]
						require.Equal(rt, expected[permission][id], actual[index] != nil)
						scalar, err := ctx.Check(it, resource, subject)
						require.NoError(rt, err)
						require.Equal(rt, expected[permission][id], scalar != nil)
					}
				}

				for _, resource := range resources {
					actual, err := ctx.CheckManySubjects(it, resource, subjects)
					require.NoError(rt, err)
					partial, err := ctx.CheckManySubjects(it, resource, subjects, AllowSingleResult)
					require.NoError(rt, err)
					assertAnyBatchEquivalent(rt, actual, partial)

					for index, subject := range subjects {
						id := resource.ObjectID[1:] + "-" + subject.ObjectID[1:]
						require.Equal(rt, expected[permission][id], actual[index] != nil)
					}
				}
			}
		}
	})
}

func TestNativeSourceGroupsRetainConditionalDirectMembership(t *testing.T) {
	converted, reader := batchTestReader(t, `caveat enabled(ok bool) { ok }
definition user {}
definition group {
 relation member: user
}
definition team {
 relation member: user
}
definition document {
 relation viewer: user with enabled | group#member | team#member
 permission view = viewer
}`, []tuple.Relationship{
		tuple.MustParse("document:partial#viewer@user:u[enabled]"),
		tuple.MustParse("document:partial#viewer@group:g#member"),
		tuple.MustParse("document:grant#viewer@user:u[enabled]"),
		tuple.MustParse("document:grant#viewer@team:t#member"),
		tuple.MustParse("team:t#member@user:u"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder(), WithDeferredCaveats())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	_, err = Walk(it, func(node Iterator) (Iterator, error) {
		if union, ok := node.(*UnionIterator); ok {
			union.checkConcurrency = 3
		}

		return node, nil
	})
	require.NoError(t, err)
	ctx := NewLocalContext(t.Context(), WithRevisionedReader(
		reader.RevisionedReader,
	), WithBatchedArrows(
		true,
	), WithCheckConcurrency(
		3,
	), WithCheckExecution(CheckExecutionOptions{
		BatchChecks: true, StrictSubjectMatching: true, BroadSingleUsersetReads: true,
	}))
	paths, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "partial"), NewObject("document", "grant")},
		NewObject("user", "u").WithEllipses(),
	)
	require.NoError(t, err)
	require.NotNil(t, paths[0])
	require.NotNil(t, paths[0].Caveat, "an unresolved direct caveat remains conditional after indirect denial")
	require.NotNil(t, paths[1])
	require.Nil(t, paths[1].Caveat, "an indirect concrete grant supersedes conditional direct membership")
}
