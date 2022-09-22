package namespace

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestListReferencedNamespaces(t *testing.T) {
	testCases := []struct {
		name          string
		toCheck       []*core.NamespaceDefinition
		expectedNames []string
	}{
		{
			"basic namespace",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.Relation("owner", nil),
				),
			},
			[]string{"document"},
		},
		{
			"basic namespaces",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.Relation("viewer", nil, ns.AllowedRelation("user", "...")),
				),
				ns.Namespace("user"),
			},
			[]string{"document", "user"},
		},
		{
			"basic namespaces with references",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.Relation("viewer", nil,
						ns.AllowedRelation("group", "member"),
						ns.AllowedRelation("group", "manager"),
						ns.AllowedRelation("team", "member")),
				),
				ns.Namespace("user"),
			},
			[]string{"document", "user", "group", "team"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			found := ListReferencedNamespaces(tc.toCheck)
			sort.Strings(found)
			sort.Strings(tc.expectedNames)

			require.Equal(tc.expectedNames, found)
		})
	}
}
