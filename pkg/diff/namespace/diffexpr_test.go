package namespace

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

func TestDiffExpressions(t *testing.T) {
	tcs := []struct {
		name     string
		existing string
		updated  string
		expected string
	}{
		{
			name:     "no change",
			existing: `viewer`,
			updated:  `viewer`,
			expected: `expression-unchanged
`,
		},
		{
			name:     "expression added",
			existing: `viewer`,
			updated:  `viewer + editor`,
			expected: `children-changed
	operation-added`,
		},
		{
			name:     "expression removed",
			existing: `viewer + editor`,
			updated:  `viewer`,
			expected: `children-changed
	operation-removed`,
		},
		{
			name:     "computed userset expression changed",
			existing: `viewer`,
			updated:  `editor`,
			expected: `children-changed
	operation-computed-userset-changed`,
		},
		{
			name:     "arrow computed userset changed",
			existing: `viewer->foo`,
			updated:  `viewer->bar`,
			expected: `children-changed
	operation-computed-userset-changed`,
		},
		{
			name:     "arrow tupleset changed",
			existing: `viewer->bar`,
			updated:  `editor->bar`,
			expected: `children-changed
	operation-tupleset-changed`,
		},
		{
			name:     "arrow unchanged",
			existing: `viewer->bar`,
			updated:  `viewer->bar`,
			expected: `expression-unchanged
`,
		},
		{
			name:     "intersection arrow tupleset changed",
			existing: `viewer.all(bar)`,
			updated:  `editor.all(bar)`,
			expected: `children-changed
	operation-tupleset-changed`,
		},
		{
			name:     "intersection arrow computed userset changed",
			existing: `viewer.all(foo)`,
			updated:  `viewer.all(bar)`,
			expected: `children-changed
	operation-computed-userset-changed`,
		},
		{
			name:     "nested expression changed",
			existing: `viewer + (editor - (foo + bar))`,
			updated:  `viewer + (editor - (foo + var))`,
			expected: `children-changed
	childexpr-children-changed
	childexpr-children-changed
	operation-computed-userset-changed`,
		},
		{
			name:     "nested expression unchanged",
			existing: `viewer + (editor - (foo + bar))`,
			updated:  `viewer + (editor - (foo + bar))`,
			expected: `expression-unchanged
`,
		},
		{
			name:     "item added to intersection",
			existing: `viewer & editor`,
			updated:  `viewer & editor & admin`,
			expected: `children-changed
	operation-added`,
		},
		{
			name:     "intersection added",
			existing: `viewer`,
			updated:  `viewer & editor`,
			expected: `operation-expanded
	operation-added`,
		},
		{
			name:     "exclusion added",
			existing: `viewer`,
			updated:  `viewer - editor`,
			expected: `operation-expanded
	operation-added`,
		},
		{
			name:     "item added to exclusion",
			existing: `viewer - editor`,
			updated:  `viewer - editor - banned`,
			expected: `children-changed
	operation-type-changed
	operation-computed-userset-changed`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parsedExisting, err := parseUsersetRewrite(tc.existing)
			require.NoError(t, err)

			parsedUpdated, err := parseUsersetRewrite(tc.updated)
			require.NoError(t, err)

			diff, err := DiffExpressions(parsedExisting, parsedUpdated)
			require.NoError(t, err)

			require.Equal(t, tc.expected, diffToString(diff))
		})
	}
}

func diffToString(diff *ExpressionDiff) string {
	childDiffStrings := make([]string, 0, len(diff.childDiffs))
	for _, childDiff := range diff.childDiffs {
		childDiffStrings = append(childDiffStrings, "\t"+opDiffToString(childDiff))
	}

	return string(diff.change) + "\n" + strings.Join(childDiffStrings, "\n")
}

func opDiffToString(diff *OperationDiff) string {
	if diff.childExprDiff != nil {
		return "childexpr-" + diffToString(diff.childExprDiff)
	}

	return string(diff.change)
}

func parseUsersetRewrite(s string) (*core.UsersetRewrite, error) {
	schema := fmt.Sprintf(`definition resource {
		permission someperm = %s
	}`, s)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: "test", SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		return nil, err
	}

	return compiled.ObjectDefinitions[0].Relation[0].UsersetRewrite, nil
}
