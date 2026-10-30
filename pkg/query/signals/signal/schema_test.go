package signal_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestSchemaCompatibilityFollowsDeclaredDependencies(t *testing.T) {
	const original = `definition user {}

caveat gate(flag bool) { flag }

definition document {
    relation viewer: user with gate
    permission view = viewer
}`

	footprint := signal.Footprint{
		Elements: []signal.Element{
			{Namespace: "document", Relation: "viewer"},
			{Namespace: "document", Relation: "view"},
		},
		Caveats: []string{"gate"},
	}
	renamedViewer := strings.Replace(original, "relation viewer:", "relation editor:", 1)
	renamedViewer = strings.Replace(renamedViewer, "= viewer", "= editor", 1)
	withoutCaveat := strings.Replace(original, "caveat gate(flag bool) { flag }", "", 1)
	withoutCaveat = strings.Replace(withoutCaveat, " with gate", "", 1)
	from := querysignalstest.Schema(t, "from", original).Get().GetV1()

	for _, tc := range []struct {
		name string
		next string
		want signal.TransferAction
	}{
		{"unchanged", original, signal.Retain},
		{"unrelated definition", original + "\ndefinition other {}", signal.Retain},
		{"definition removed", "definition user {}\ncaveat gate(flag bool) { flag }", signal.Reset},
		{"relation removed", renamedViewer, signal.Reset},
		{"unrelated relation", strings.Replace(original, "permission view", "relation editor: user\npermission view", 1), signal.Retain},
		{"changed caveat", strings.Replace(original, "{ flag }", "{ !flag }", 1), signal.Reset},
		{"removed caveat", withoutCaveat, signal.Reset},
		{"definition comment", strings.Replace(original, "definition document", "/** Resources. */\ndefinition document", 1), signal.Retain},
		{"caveat comment", strings.Replace(original, "caveat gate", "/** Context gate. */\ncaveat gate", 1), signal.Retain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			to := querysignalstest.Schema(t, "to", tc.next).Get().GetV1()
			changes, err := diff.DiffSchemas(
				diff.DiffableSchema{
					ObjectDefinitions: slices.Collect(maps.Values(from.NamespaceDefinitions)),
					CaveatDefinitions: slices.Collect(maps.Values(from.CaveatDefinitions)),
				},
				diff.DiffableSchema{
					ObjectDefinitions: slices.Collect(maps.Values(to.NamespaceDefinitions)),
					CaveatDefinitions: slices.Collect(maps.Values(to.CaveatDefinitions)),
				},
				caveattypes.Default.TypeSet,
			)

			require.NoError(t, err)
			decision := signal.Unchanged(signal.SchemaChange{Diff: changes}, footprint)

			require.Equal(t, tc.want, decision.Action)
		})
	}

	require.Equal(t, signal.Reset, signal.Unchanged(signal.SchemaChange{}, footprint).Action)
	require.Equal(t, signal.Reset, signal.Unchanged(signal.SchemaChange{Diff: &diff.SchemaDiff{}}, signal.Footprint{}).Action)
}
