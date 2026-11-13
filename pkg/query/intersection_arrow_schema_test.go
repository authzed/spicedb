package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestHeterogeneousAllRequiresEveryActualTarget(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		text := `definition user {}
definition group {
 relation member: user
}
definition team {
 relation member: user
}
definition empty {}
definition document {
 relation parent: group | team | empty
 permission view = parent.all(member)
}`
		relationships := []tuple.Relationship{}
		targetCount := 0
		allGrant := true

		// Derive the expected answer directly from generated edges, independently
		// of the schema builder and both scalar/batch execution implementations.
		for _, kind := range []string{"group", "team", "empty"} {
			for index := range 2 {
				label := fmt.Sprintf("%s-%d", kind, index)
				if !rapid.Bool().Draw(rt, "present-"+label) {
					continue
				}

				targetCount++
				relationships = append(relationships, tuple.MustParse("document:d#parent@"+kind+":"+label))
				grants := kind != "empty" && rapid.Bool().Draw(rt, "grants-"+label)
				allGrant = allGrant && grants

				if grants {
					relationships = append(relationships, tuple.MustParse(kind+":"+label+"#member@user:u"))
				}
			}
		}

		converted, reader := batchTestReader(rt, text, relationships)
		outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder())
		require.NoError(rt, err)
		it, err := outline.Compile()
		require.NoError(rt, err)
		expected := targetCount > 0 && allGrant
		resource := NewObject("document", "d")
		subject := NewObject("user", "u").WithEllipses()

		for _, batched := range []bool{false, true} {
			ctx := NewLocalContext(rt.Context(), WithRevisionedReader(reader), WithBatchedArrows(batched),
				WithCheckExecution(
					CheckExecutionOptions{
						BatchChecks:                  batched,
						ExhaustiveIntersectionArrows: batched,
						UnfilteredSingleTypeReads:    true,
						BroadSingleUsersetReads:      true,
					},
				))
			path, err := ctx.Check(it, resource, subject)
			require.NoError(rt, err)
			require.Equal(rt, expected, path != nil, "batched=%v", batched)

			seq, err := ctx.IterSubjects(it, resource, ObjectType{Type: "user", Subrelation: "..."})
			require.NoError(rt, err)
			paths, err := CollectAll(seq)
			require.NoError(rt, err)
			require.Equal(rt, expected, len(paths) > 0)

			seq, err = ctx.IterResources(it, subject, ObjectType{Type: "document"})
			require.NoError(rt, err)
			paths, err = CollectAll(seq)
			require.NoError(rt, err)
			require.Equal(rt, expected, len(paths) > 0)
		}
	})
}
