package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

func hasOptimizer(opts []Optimizer, name string) bool {
	for _, opt := range opts {
		if opt.Name == name {
			return true
		}
	}
	return false
}

func TestOptimizersForRequest(t *testing.T) {
	tests := []struct {
		name            string
		params          RequestParams
		includeCollapse bool
	}{
		{
			name:            "Check with empty subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: ""},
			includeCollapse: true,
		},
		{
			name:            "Check with specific subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: "viewer"},
			includeCollapse: true,
		},
		{
			name:            "Check with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterResources with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterResources with specific subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: "viewer"},
			includeCollapse: false,
		},
		{
			name:            "IterResources with empty subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: ""},
			includeCollapse: false,
		},
		{
			name:            "IterSubjects with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationIterSubjects, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterSubjects with specific subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterSubjects, SubjectRelation: "owner"},
			includeCollapse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := OptimizersForRequest(tc.params)
			if tc.includeCollapse {
				require.True(t, hasOptimizer(opts, "alias-chain-collapse"))
			} else {
				require.False(t, hasOptimizer(opts, "alias-chain-collapse"))
			}
		})
	}
}
